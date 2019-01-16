package mydump

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pkg/errors"
)

var (
	// errors
	errDirNotExists = errors.New("mydumper dir not exists")
	errMissingFile  = errors.New("missing file")
)

type MDDatabaseMeta struct {
	Name       string
	SchemaFile string
	Tables     []*MDTableMeta
	charSet    string
}

func (m *MDDatabaseMeta) String() string {
	v, err := json.Marshal(m)
	if err != nil {
		common.AppLogger.Errorf("json marshal MDDatabaseMeta error %s", errors.ErrorStack(err))
	}
	return string(v)
}

func (m *MDDatabaseMeta) GetSchema() string {
	schema, err := ExportStatement(m.SchemaFile, m.charSet)
	if err != nil {
		common.AppLogger.Errorf("failed to extract database schema (%s) : %s", m.SchemaFile, err.Error())
		return ""
	}
	return string(schema)
}

type MDTableMeta struct {
	DB         string
	Name       string
	SchemaFile string
	DataFiles  []string
	charSet    string
}

func (m *MDTableMeta) GetSchema() string {
	schema, err := ExportStatement(m.SchemaFile, m.charSet)
	if err != nil {
		common.AppLogger.Errorf("failed to extract table schema (%s) : %s", m.SchemaFile, err.Error())
		return ""
	}
	return string(schema)
}

/*
	Mydumper File Loader
*/
type MDLoader struct {
	dir      string
	noSchema bool
	dbs      []*MDDatabaseMeta
	filter   *filter.Filter
	charSet  string
}

type mdLoaderSetup struct {
	loader        *MDLoader
	dbSchemas     []fileInfo
	tableSchemas  []fileInfo
	tableDatas    []fileInfo
	dbIndexMap    map[string]int
	tableIndexMap map[filter.Table]int
}

func NewMyDumpLoader(cfg *config.Config) (*MDLoader, error) {
	mdl := &MDLoader{
		dir:      cfg.Mydumper.SourceDir,
		noSchema: cfg.Mydumper.NoSchema,
		filter:   filter.New(false, cfg.BWList),
		charSet:  cfg.Mydumper.CharacterSet,
	}

	setup := mdLoaderSetup{
		loader:        mdl,
		dbIndexMap:    make(map[string]int),
		tableIndexMap: make(map[filter.Table]int),
	}

	if err := setup.setup(mdl.dir); err != nil {
		// common.AppLogger.Errorf("init mydumper loader failed : %s\n", err.Error())
		return nil, errors.Trace(err)
	}

	return mdl, nil
}

type fileType int

const (
	fileTypeDatabaseSchema fileType = iota
	fileTypeTableSchema
	fileTypeTableDataSQL
)

func (ftype fileType) String() string {
	switch ftype {
	case fileTypeDatabaseSchema:
		return "database schema"
	case fileTypeTableSchema:
		return "table schema"
	case fileTypeTableDataSQL:
		return "table data SQL"
	default:
		return "(unknown)"
	}
}

type fileInfo struct {
	tableName filter.Table
	path      string
}

var tableNameRegexp = regexp.MustCompile(`^([^.]+)\.(.*?)(?:\.[0-9]+)?$`)

// setup the `s.loader.dbs` slice by scanning all *.sql files inside `dir`.
//
// The database and tables are inserted in a consistent order, so creating an
// MDLoader twice with the same data source is going to produce the same array,
// even after killing Lightning.
//
// This is achieved by using `filepath.Walk` internally which guarantees the
// files are visited in lexicographical order (note that this does not mean the
// databases and tables in the end are ordered lexicographically since they may
// be stored in different subdirectories).
func (s *mdLoaderSetup) setup(dir string) error {
	/*
		Mydumper file names format
			db    —— {db}-schema-create.sql
			table —— {db}.{table}-schema.sql
			sql   —— {db}.{table}.{part}.sql / {db}.{table}.sql
	*/
	if !common.IsDirExists(dir) {
		return errors.Annotatef(errDirNotExists, "dir %s", dir)
	}

	if err := s.listFiles(dir); err != nil {
		common.AppLogger.Errorf("list file failed : %s", err.Error())
		return errors.Trace(err)
	}

	if !s.loader.noSchema {
		// setup database schema
		if len(s.dbSchemas) == 0 {
			return errors.Annotatef(errMissingFile, "missing {schema}-schema-create.sql")
		}
		for _, fileInfo := range s.dbSchemas {
			if _, dbExists := s.insertDB(fileInfo.tableName.Schema, fileInfo.path); dbExists {
				return errors.Errorf("invalid database schema file, duplicated item - %s", fileInfo.path)
			}
		}

		// setup table schema
		for _, fileInfo := range s.tableSchemas {
			_, dbExists, tableExists := s.insertTable(fileInfo.tableName, fileInfo.path)
			if !dbExists {
				return errors.Errorf("invalid table schema file, cannot find db - %s", fileInfo.path)
			} else if tableExists {
				return errors.Errorf("invalid table schema file, duplicated item - %s", fileInfo.path)
			}
		}
	}

	// Sql file for restore data
	for _, fileInfo := range s.tableDatas {
		tableMeta, dbExists, tableExists := s.insertTable(fileInfo.tableName, "")
		if !s.loader.noSchema {
			if !dbExists {
				return errors.Errorf("invalid data file, miss host db - %s", fileInfo.path)
			} else if !tableExists {
				return errors.Errorf("invalid data file, miss host table - %s", fileInfo.path)
			}
		}
		tableMeta.DataFiles = append(tableMeta.DataFiles, fileInfo.path)
	}

	return nil
}

func (s *mdLoaderSetup) listFiles(dir string) error {
	// `filepath.Walk` yields the paths in a deterministic (lexicographical) order,
	// meaning the file and chunk orders will be the same everytime it is called
	// (as long as the source is immutable).
	err := filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return errors.Trace(err)
		}

		if f == nil || f.IsDir() {
			return nil
		}

		fname := strings.TrimSpace(f.Name())
		info := fileInfo{path: path}

		var (
			ftype         fileType
			qualifiedName string
		)
		switch {
		case strings.HasSuffix(fname, "-schema-create.sql"):
			ftype = fileTypeDatabaseSchema
			qualifiedName = fname[:len(fname)-18] + "."

		case strings.HasSuffix(fname, "-schema.sql"):
			ftype = fileTypeTableSchema
			qualifiedName = fname[:len(fname)-11]

			// ignore functionality :
			// 		- view
			//		- triggers
		case strings.HasSuffix(fname, "-schema-view.sql"),
			strings.HasSuffix(fname, "-schema-trigger.sql"),
			strings.HasSuffix(fname, "-schema-post.sql"):
			common.AppLogger.Warn("[loader] ignore unsupport view/trigger:", path)
			return nil
		case strings.HasSuffix(fname, ".sql"):
			ftype = fileTypeTableDataSQL
			qualifiedName = fname[:len(fname)-4]
		default:
			return nil
		}

		matchRes := tableNameRegexp.FindStringSubmatch(qualifiedName)
		if len(matchRes) != 3 {
			common.AppLogger.Debugf("[loader] ignore almost %s file: %s", ftype, path)
			return nil
		}
		info.tableName.Schema = matchRes[1]
		info.tableName.Name = matchRes[2]

		if s.loader.shouldSkip(&info.tableName) {
			common.AppLogger.Infof("[filter] ignoring table file %s", path)
			return nil
		}

		switch ftype {
		case fileTypeDatabaseSchema:
			s.dbSchemas = append(s.dbSchemas, info)
		case fileTypeTableSchema:
			s.tableSchemas = append(s.tableSchemas, info)
		case fileTypeTableDataSQL:
			s.tableDatas = append(s.tableDatas, info)
		}
		return nil
	})

	return errors.Trace(err)
}

func (l *MDLoader) shouldSkip(table *filter.Table) bool {
	return len(l.filter.ApplyOn([]*filter.Table{table})) == 0
}

func (s *mdLoaderSetup) insertDB(dbName string, path string) (*MDDatabaseMeta, bool) {
	dbIndex, ok := s.dbIndexMap[dbName]
	if ok {
		return s.loader.dbs[dbIndex], true
	} else {
		s.dbIndexMap[dbName] = len(s.loader.dbs)
		ptr := &MDDatabaseMeta{
			Name:       dbName,
			SchemaFile: path,
			charSet:    s.loader.charSet,
		}
		s.loader.dbs = append(s.loader.dbs, ptr)
		return ptr, false
	}
}

func (s *mdLoaderSetup) insertTable(tableName filter.Table, path string) (*MDTableMeta, bool, bool) {
	dbMeta, dbExists := s.insertDB(tableName.Schema, "")
	tableIndex, ok := s.tableIndexMap[tableName]
	if ok {
		return dbMeta.Tables[tableIndex], dbExists, true
	} else {
		s.tableIndexMap[tableName] = len(dbMeta.Tables)
		ptr := &MDTableMeta{
			DB:         tableName.Schema,
			Name:       tableName.Name,
			SchemaFile: path,
			DataFiles:  make([]string, 0, 16),
			charSet:    s.loader.charSet,
		}
		dbMeta.Tables = append(dbMeta.Tables, ptr)
		return ptr, dbExists, false
	}
}

func (l *MDLoader) GetDatabases() []*MDDatabaseMeta {
	return l.dbs
}
