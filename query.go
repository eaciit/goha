package goha

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/toolkit"
)

type Query struct {
	dbflex.QueryBase
}

func (q *Query) Cursor(in toolkit.M) dbflex.ICursor {
	cur := new(Cursor)
	cur.SetThis(cur)

	//tableName := q.Config(dbflex.ConfigKeyTableName, "").(string)
	//items := q.Config(dbflex.ConfigKeyGroupedQueryItems, dbflex.GroupedQueryItems{}).(dbflex.GroupedQueryItems)
	cmdType := q.Config(dbflex.ConfigKeyCommandType, dbflex.QuerySelect).(string)
	tableName := q.Config(dbflex.ConfigKeyTableName, "").(string)
	//familyName := in.Get("family", "def").(string)

	if cmdType != dbflex.QuerySelect {
		cur.SetError(fmt.Errorf("cursor could not be run using %s operation", cmdType))
		return cur
	}

	where := q.Config(dbflex.ConfigKeyWhere, nil)

	client := q.Connection().(*Connection).client
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	var (
		scan *hrpc.Scan
		err  error
	)
	if where == nil {
		scan, err = hrpc.NewScan(ctx, []byte(tableName))
	} else {
		//fmt.Printf("hrpc call: error\n")
		scan, err = hrpc.NewScanStr(ctx, tableName, where.(func(hrpc.Call) error))
	}

	if err != nil {
		cur.SetError(fmt.Errorf("unable to prepare scan. %s", err.Error()))
		return cur
	}

	cur.scanner = client.Scan(scan)
	return cur
}

func (q *Query) Execute(in toolkit.M) (interface{}, error) {
	client := q.Connection().(*Connection).client
	admin := q.Connection().(*Connection).admin
	cmdtype := q.Config(dbflex.ConfigKeyCommandType, "")
	tableName := q.Config(dbflex.ConfigKeyTableName, "").(string)

	data, hasData := in["data"]
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	//ctx := context.Background()

	gitems := q.Config(dbflex.ConfigKeyGroupedQueryItems, dbflex.GroupedQueryItems{}).(dbflex.GroupedQueryItems)

	switch cmdtype {
	case dbflex.QueryCommand:
		cmdname := gitems[dbflex.QueryCommand][0].Value.(toolkit.M).Get("command", "")
		switch cmdname {
		case "create-table":
			families := in.Get("families", []string{"def"}).([]string)
			mapFamilies := make(map[string]map[string]string, len(families))
			for _, f := range families {
				mapFamilies[f] = nil
			}
			hrpcTable := hrpc.NewCreateTable(ctx, []byte(tableName), mapFamilies)
			e := admin.CreateTable(hrpcTable)
			return nil, e

		case "delete-table":
			dit := hrpc.NewDisableTable(ctx, []byte(tableName))
			err := admin.DisableTable(dit)
			if err != nil {
				if !strings.Contains(err.Error(), "TableNotEnabledException") {
					return nil, err
				}
			}

			det := hrpc.NewDeleteTable(context.Background(), []byte(tableName))
			err = admin.DeleteTable(det)
			if err != nil {
				return nil, err
			}
			return nil, nil

		default:
			return nil, fmt.Errorf("invalid command: %s", cmdname)
		}

	case dbflex.QueryInsert:
		if !hasData {
			return nil, fmt.Errorf("hbase insert error: no data specified")
		}
		family := in.Get("family", "def").(string)
		idfieldname := in.Get("idfieldname", "").(string)
		mut, err := toHbasePutStr(ctx, tableName, "", idfieldname, family, data)
		if err != nil {
			return nil, fmt.Errorf("hbase insert error, unable to serialize data. %s", err.Error())
		}
		res, err := client.Put(mut)
		return res, nil
	}
	return nil, fmt.Errorf("%s is not yet implemented for this driver", "Execute")
}

func (q *Query) BuildFilter(f *dbflex.Filter) (interface{}, error) {
	hf := filter.NewCompareFilter(filter.Equal, filter.NewBinaryComparator(
		filter.NewByteArrayComparable([]byte("key-user-1"))))

	filters := hrpc.Filters(hf)
	return filters, nil
}

func (q *Query) BuildCommand() (interface{}, error) {
	return nil, nil
}

func toHbasePutStr(ctx context.Context, table, key, idfieldname, family string, data interface{}) (*hrpc.Mutate, error) {
	hbaseData := map[string]map[string][]byte{}
	rv := reflect.Indirect(reflect.ValueOf(data))
	rt := rv.Type()
	if family == "" {
		family = "def"
	}
	if key == "" {
		idField := rv.FieldByName(idfieldname)
		if !idField.IsValid() {
			return nil, fmt.Errorf("key on field %s is not valid", idfieldname)
		} else if rv.FieldByName(idfieldname).Kind() != reflect.String {
			return nil, fmt.Errorf("ID field %s should be string", idfieldname)
		}
		key = idField.String()
	}

	familyData := map[string][]byte{}
	elemCount := rt.NumField()
	for i := 0; i < elemCount; i++ {
		elemType := rt.Field(i)
		elem := rv.Field(i)
		fieldName := elemType.Name

		if fieldName != idfieldname {
			elemTypeString := elemType.Type.String()
			if elemTypeString != "interface{}" && strings.HasPrefix(elemTypeString, "int") {
				familyData[fieldName] = int64ToByte(elem.Int())
			} else if strings.HasPrefix(elemTypeString, "float") {
				//fmt.Printf("Salaryinsert: %v\n", elem.Float())
				familyData[fieldName] = float64ToByte(elem.Float())
			} else if elemTypeString == "*time.Time" {
				t := elem.Interface().(time.Time)
				i := t.UnixNano()
				familyData[fieldName] = int64ToByte(i)
			} else if elemTypeString == "time.Time" {
				t := elem.Interface().(*time.Time)
				i := t.UnixNano()
				familyData[fieldName] = int64ToByte(i)
			} else {
				familyData[fieldName] = []byte(elem.String())
				//familyData[fieldName] = []byte("Hello all")
			}
		}
	}
	hbaseData[family] = familyData

	//fmt.Println("data:", hbaseData)
	putStr, err := hrpc.NewPutStr(ctx, table, key, hbaseData)
	return putStr, err
}

func float64ToByte(f float64) []byte {
	bits := math.Float64bits(f)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

func int64ToByte(i int64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, uint64(i))
	return bs
}
