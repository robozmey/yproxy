package database

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/pkg/errors"
	"github.com/yezzey-gp/yproxy/pkg/ylogger"
)

//go:generate mockgen -destination=../mock/mock_database_interractor.go -package mock
type DatabaseInterractor interface {
	GetVirtualExpireIndexes(port uint64) (map[string]bool, map[string]uint64, error)
}

type DatabaseHandler struct {
}

type DB struct {
	name       string
	tablespace pgtype.OID
	oid        pgtype.OID
}

type ExpireHint struct {
	expireLsn string
	x_path    string
}

func (database *DatabaseHandler) populateIndex() {

}

func (database *DatabaseHandler) GetVirtualExpireIndexes(port uint64) (map[string]bool, map[string]uint64, error) { //TODO несколько баз
	db, err := getDatabase(port)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get ao/aocs tables %v", err) //fix
	}
	ylogger.Zero.Debug().Str("database name", db.name).Msg("recieved database")
	conn, err := connectToDatabase(port, db.name)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close() //error
	ylogger.Zero.Debug().Msg("connected to database")

	c := make(map[string]uint64, 0)

	/* Todo: check that yezzey version >= 1.8.1 */
	if false {
		rows, err := conn.Query(`SELECT x_path, expire_lsn FROM yezzey.yezzey_expire_hint;`)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to get ao/aocs tables %v", err) //fix
		}
		defer rows.Close()
		ylogger.Zero.Debug().Msg("executed select")

		for rows.Next() {
			row := ExpireHint{}
			if err := rows.Scan(&row.x_path, &row.expireLsn); err != nil {
				return nil, nil, fmt.Errorf("unable to parse query output %v", err)
			}

			lsn, err := pgx.ParseLSN(row.expireLsn)
			if err != nil {
				return nil, nil, fmt.Errorf("unable to parse query output %v", err)
			}

			ylogger.Zero.Debug().Str("x_path", row.x_path).Str("lsn", row.expireLsn).Msg("added file to expire hint")
			c[row.x_path] = lsn
		}
		ylogger.Zero.Debug().Msg("fetched expire hint info")
	}

	rows2, err := conn.Query(`SELECT x_path FROM yezzey.yezzey_virtual_index;`)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get ao/aocs tables %v", err) //fix
	}
	defer rows2.Close()

	c2 := make(map[string]bool, 0)
	for rows2.Next() {
		xpath := ""
		if err := rows2.Scan(&xpath); err != nil {
			return nil, nil, fmt.Errorf("unable to parse query output %v", err)
		}
		c2[xpath] = true
		ylogger.Zero.Debug().Str("x_path", xpath).Msg("added")
	}
	ylogger.Zero.Debug().Msg("fetched virtual index  info")

	return c2, c, err
}

func getDatabase(port uint64) (DB, error) {
	conn, err := connectToDatabase(port, "postgres")
	if err != nil {
		return DB{}, err
	}
	defer conn.Close() //error
	ylogger.Zero.Debug().Msg("connected to db")
	rows, err := conn.Query(`SELECT dattablespace, oid, datname FROM pg_database WHERE datallowconn;`)
	if err != nil {
		return DB{}, err
	}
	defer rows.Close()
	ylogger.Zero.Debug().Msg("recieved db list")

	for rows.Next() {
		row := DB{}
		ylogger.Zero.Debug().Msg("cycle 1")
		if err := rows.Scan(&row.tablespace, &row.oid, &row.name); err != nil {
			return DB{}, err
		}
		ylogger.Zero.Debug().Msg("cycle 2")
		ylogger.Zero.Debug().Str("db", row.name).Int("db", int(row.oid)).Int("db", int(row.tablespace)).Msg("database")
		if row.name == "postgres" {
			continue
		}

		ylogger.Zero.Debug().Str("db", row.name).Msg("check database")
		connDb, err := connectToDatabase(port, row.name)
		if err != nil {
			return DB{}, err
		}
		defer connDb.Close() //error
		ylogger.Zero.Debug().Msg("cycle 3")

		rowsdb, err := connDb.Query(`SELECT exists(SELECT * FROM information_schema.schemata WHERE schema_name='yezzey');`)
		if err != nil {
			return DB{}, err
		}
		defer rowsdb.Close()
		ylogger.Zero.Debug().Msg("cycle 4")
		var ans bool
		rowsdb.Next()
		err = rowsdb.Scan(&ans)
		if err != nil {
			ylogger.Zero.Error().AnErr("error", err).Msg("error during yezzey check")
			return DB{}, err
		}
		ylogger.Zero.Debug().Bool("result", ans).Msg("find yezzey schema")
		if ans {
			ylogger.Zero.Debug().Str("db", row.name).Msg("found yezzey schema in database")
			ylogger.Zero.Debug().Int("db", int(row.oid)).Int("db", int(row.tablespace)).Msg("found yezzey schema in database")
			return row, nil
		}

		ylogger.Zero.Debug().Str("db", row.name).Msg("no yezzey schema in database")
	}
	return DB{}, fmt.Errorf("no yezzey schema across databases")
}

func connectToDatabase(port uint64, database string) (*pgx.Conn, error) {
	config, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, errors.Wrap(err, "Connect: unable to read environment variables")
	}

	config.Port = uint16(port)
	config.Database = database

	config.RuntimeParams["gp_role"] = "utility"
	conn, err := pgx.Connect(config)
	if err != nil {
		config.RuntimeParams["gp_session_role"] = "utility"
		conn, err = pgx.Connect(config)
		if err != nil {
			fmt.Printf("error in connection %v", err) // delete this
			return nil, err
		}
	}
	return conn, nil
}
