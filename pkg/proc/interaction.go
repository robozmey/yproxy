package proc

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/yezzey-gp/yproxy/pkg/client"
	"github.com/yezzey-gp/yproxy/pkg/crypt"
	"github.com/yezzey-gp/yproxy/pkg/message"
	"github.com/yezzey-gp/yproxy/pkg/storage"
	"github.com/yezzey-gp/yproxy/pkg/ylogger"
)

type YproxyRetryReader struct {
	underlying io.ReadCloser

	bytesWrite        int64
	retryCnt          int64
	retryLimit        int
	needReacquire     bool
	reacquireReaderFn func(offsetStart int64) (io.ReadCloser, error)
}

// Close implements io.ReadCloser.
func (y *YproxyRetryReader) Close() error {
	err := y.underlying.Close()
	if err != nil {
		ylogger.Zero.Error().Err(err).Msg("encounter close error")
	}
	return err
}

// Read implements io.ReadCloser.
func (y *YproxyRetryReader) Read(p []byte) (int, error) {

	for retry := 0; retry < y.retryLimit; retry++ {

		if y.needReacquire {

			r, err := y.reacquireReaderFn(y.bytesWrite)

			if err != nil {
				// log error and continue.
				// Try to mitigate overload problems with random sleep
				ylogger.Zero.Error().Err(err).Int("offset reached", int(y.bytesWrite)).Int("retry count", int(retry)).Msg("failed to reacquire external storage connection, wait and retry")

				time.Sleep(time.Second)
				continue
			}
			//
			y.underlying = r

			y.needReacquire = false
		}

		n, err := y.underlying.Read(p)
		if err == io.EOF {
			return n, err
		}
		if err != nil || n < 0 {
			ylogger.Zero.Error().Err(err).Int("offset reached", int(y.bytesWrite)).Int("retry count", int(retry)).Msg("encounter read error")

			// what if close failed?
			_ = y.underlying.Close()

			// try to reacquire connection to external storage and continue read
			// from previously reached point

			y.needReacquire = true
			continue
		} else {
			y.bytesWrite += int64(n)

			return n, err
		}
	}
	return -1, fmt.Errorf("failed to unpload within retries")
}

const (
	defaultRetryLimit = 100
)

func newYRetryReader(reacquireReaderFn func(offsetStart int64) (io.ReadCloser, error)) io.ReadCloser {
	return &YproxyRetryReader{
		reacquireReaderFn: reacquireReaderFn,
		retryLimit:        defaultRetryLimit,
		bytesWrite:        0,
		needReacquire:     true,
	}
}

var _ io.ReadCloser = &YproxyRetryReader{}

func ProcConn(s storage.StorageInteractor, cr crypt.Crypter, ycl *client.YClient) error {

	defer func() {
		_ = ycl.Conn.Close()
	}()

	pr := NewProtoReader(ycl)
	tp, body, err := pr.ReadPacket()
	if err != nil {
		_ = ycl.ReplyError(err, "failed to read request packet")
		return err
	}

	ylogger.Zero.Debug().Str("msg-type", tp.String()).Msg("recieved client request")

	switch tp {
	case message.MessageTypeCat:
		// omit first byte
		msg := message.CatMessage{}
		msg.Decode(body)

		yr := newYRetryReader(
			func(offsetStart int64) (io.ReadCloser, error) {
				ylogger.Zero.Debug().Str("object-path", msg.Name).Int64("offset", offsetStart).Msg("cat object with offset")
				r, err := s.CatFileFromStorage(msg.Name, offsetStart)
				if err != nil {
					return nil, err
				}

				return r, nil
			},
		)

		var contentReader io.Reader
		contentReader = yr
		defer yr.Close()

		if msg.Decrypt {
			ylogger.Zero.Debug().Str("object-path", msg.Name).Msg("decrypt object")
			contentReader, err = cr.Decrypt(yr)
			if err != nil {
				_ = ycl.ReplyError(err, "failed to decrypt object")

				return err
			}
		}
		_, err = io.Copy(ycl.Conn, contentReader)
		if err != nil {
			_ = ycl.ReplyError(err, "copy failed to compelete")
		}

	case message.MessageTypePut:

		msg := message.PutMessage{}
		msg.Decode(body)

		var w io.WriteCloser

		r, w := io.Pipe()

		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {

			var ww io.WriteCloser = w
			if msg.Encrypt {
				var err error
				ww, err = cr.Encrypt(w)
				if err != nil {
					_ = ycl.ReplyError(err, "failed to encrypt")

					ycl.Conn.Close()
					return
				}
			}

			defer w.Close()
			defer wg.Done()

			for {
				tp, body, err := pr.ReadPacket()
				if err != nil {
					_ = ycl.ReplyError(err, "failed to read chunk of data")
					return
				}

				ylogger.Zero.Debug().Str("msg-type", tp.String()).Msg("recieved client request")

				switch tp {
				case message.MessageTypeCopyData:
					msg := message.CopyDataMessage{}
					msg.Decode(body)
					if n, err := ww.Write(msg.Data); err != nil {
						_ = ycl.ReplyError(err, "failed to write copy data")

						return
					} else if n != int(msg.Sz) {

						_ = ycl.ReplyError(fmt.Errorf("unfull write"), "failed to compelete request")

						return
					}
				case message.MessageTypeCommandComplete:
					msg := message.CommandCompleteMessage{}
					msg.Decode(body)

					if err := ww.Close(); err != nil {
						_ = ycl.ReplyError(err, "failed to close connection")
						return
					}

					ylogger.Zero.Debug().Msg("closing msg writer")
					return
				}
			}
		}()

		err := s.PutFileToDest(msg.Name, r)

		wg.Wait()

		if err != nil {
			_ = ycl.ReplyError(err, "failed to upload")

			return nil
		}

		_, err = ycl.Conn.Write(message.NewReadyForQueryMessage().Encode())

		if err != nil {
			_ = ycl.ReplyError(err, "failed to upload")

			return nil
		}

	case message.MessageTypeList:
		msg := message.ListMessage{}
		msg.Decode(body)

		objectMetas, err := s.ListPath(msg.Prefix)
		if err != nil {
			_ = ycl.ReplyError(fmt.Errorf("could not list objects: %s", err), "failed to compelete request")

			return nil
		}

		const chunkSize = 1000

		for i := 0; i < len(objectMetas); i += chunkSize {
			_, err = ycl.Conn.Write(message.NewObjectMetaMessage(objectMetas[i:min(i+chunkSize, len(objectMetas))]).Encode())
			if err != nil {
				_ = ycl.ReplyError(err, "failed to upload")

				return nil
			}

		}

		_, err = ycl.Conn.Write(message.NewReadyForQueryMessage().Encode())

		if err != nil {
			_ = ycl.ReplyError(err, "failed to upload")

			return nil
		}

	default:
		_ = ycl.ReplyError(nil, "wrong request type")

		return nil
	}

	return nil
}
