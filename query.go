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

	filter := q.Config(dbflex.ConfigKeyFilter, nil)

	client := q.Connection().(*Connection).client
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	family := in.Get("family", DefaultFamilyName()).(string)
	idfieldname := in.Get("idfieldname", DefaultIDFieldName()).(string)

	cur.Set("familyname", family)
	cur.Set("idfieldname", idfieldname)

	var (
		scan *hrpc.Scan
		err  error
	)
	if filter == nil {
		scan, err = hrpc.NewScan(ctx, []byte(tableName))
	} else {
		//fmt.Printf("hrpc call: error\n")

		q.SetConfig("familyname", family)
		q.SetConfig("idfieldname", idfieldname)

		where, err := q.BuildFilter(filter.(*dbflex.Filter))
		if err != nil {
			cur.SetError(fmt.Errorf("unable build filter. %s", err.Error()))
			return cur
		}
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
			familyName := in.Get("family", DefaultFamilyName()).(string)

			families := in.Get("families", []string{familyName}).([]string)
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

	case dbflex.QueryInsert, dbflex.QueryUpdate, dbflex.QuerySave:
		if !hasData {
			return nil, fmt.Errorf("hbase insert error: no data specified")
		}
		family := in.Get("family", DefaultFamilyName()).(string)
		idfieldname := in.Get("idfieldname", DefaultIDFieldName()).(string)

		fmt.Println("ID Field Name:", idfieldname)

		mut, err := toHbaseMutate(ctx, "save", tableName, "", idfieldname, family, data)
		if err != nil {
			return nil, fmt.Errorf("hbase insert error, unable to prepare mutation. %s", err.Error())
		}
		res, err := client.Put(mut)
		return res, nil

	//-- since delete by filter is not allowed on HBase, hence deley can only be done by key
	case dbflex.QueryDelete:
		ids := in.Get(DefaultIDFieldName(), []string{}).([]string)
		family := in.Get("family", DefaultFamilyName()).(string)
		//idfieldname := in.Get("idfieldname", "").(string)
		hbfamily := map[string]map[string][]byte{family: nil}

		for _, id := range ids {
			mut, err := hrpc.NewDelStr(ctx, tableName, id, hbfamily)
			if err != nil {
				return nil, fmt.Errorf("hbase delete error, unable to prepare mutation. %s", err.Error())
			}
			_, err = client.Delete(mut)
			if err != nil {
				return nil, fmt.Errorf("hbase delete error, unable to delete %s. %s", id, err.Error())
			}
		}
		return nil, nil
	}
	return nil, fmt.Errorf("%s is not yet implemented for this driver", "Execute")
}

var defaultFamilyName = "def"

func SetDefaultFamilyName(s string) {
	defaultFamilyName = s
}

func DefaultFamilyName() string {
	return defaultFamilyName
}

func (q *Query) BuildFilter(f *dbflex.Filter) (interface{}, error) {
	idfieldname := q.Config("idfieldname", DefaultIDFieldName()).(string)
	familyName := q.Config("familyname", DefaultFamilyName()).(string)
	hbf := dbf2hbf(familyName, idfieldname, f)
	//fmt.Printf("hb filter: %v\n", toolkit.JsonString(hbf))
	filters := hrpc.Filters(hbf)
	return filters, nil
}

func dbf2hbf(familyname, idfieldname string, dbf *dbflex.Filter) filter.Filter {
	//fmt.Println("idfn0:", dbf.Field, "field:", dbf.Field)
	if dbf.Field == idfieldname {
		fmt.Println("idfn1:", dbf.Field)
		if dbf.Op == dbflex.OpEq {
			bdata := toBytes(reflect.ValueOf(dbf.Value), reflect.TypeOf(dbf.Value))
			hf := filter.NewRowFilter(filter.NewCompareFilter(filter.Equal,
				filter.NewBinaryComparator(filter.NewByteArrayComparable(bdata))))
			return hf
		} else if dbf.Op == dbflex.OpGt {
			bdata := toBytes(reflect.ValueOf(dbf.Value), reflect.TypeOf(dbf.Value))
			hf := filter.NewRowFilter(filter.NewCompareFilter(filter.Greater,
				filter.NewBinaryComparator(filter.NewByteArrayComparable(bdata))))
			return hf
		} else if dbf.Op == dbflex.OpGte {
			bdata := toBytes(reflect.ValueOf(dbf.Value), reflect.TypeOf(dbf.Value))
			hf := filter.NewRowFilter(filter.NewCompareFilter(filter.GreaterOrEqual,
				filter.NewBinaryComparator(filter.NewByteArrayComparable(bdata))))
			return hf
		} else if dbf.Op == dbflex.OpLt {
			bdata := toBytes(reflect.ValueOf(dbf.Value), reflect.TypeOf(dbf.Value))
			hf := filter.NewRowFilter(filter.NewCompareFilter(filter.Less,
				filter.NewBinaryComparator(filter.NewByteArrayComparable(bdata))))
			return hf
		} else if dbf.Op == dbflex.OpLte {
			bdata := toBytes(reflect.ValueOf(dbf.Value), reflect.TypeOf(dbf.Value))
			hf := filter.NewRowFilter(filter.NewCompareFilter(filter.LessOrEqual,
				filter.NewBinaryComparator(filter.NewByteArrayComparable(bdata))))
			return hf
		}
	} else {
		if dbf.Op == dbflex.OpAnd {
			list := filter.NewList(filter.MustPassAll)
			dbfs := dbf.Items
			for _, dbfc := range dbfs {
				hbf := dbf2hbf(familyname, idfieldname, dbfc)
				if hbf != nil {
					list.AddFilters(hbf)
				}
			}
			//fmt.Printf("List: %v\n", toolkit.JsonString(list))
			return list
		} else if dbf.Op == dbflex.OpOr {
			list := filter.NewList(filter.MustPassOne)
			dbfs := dbf.Items
			for _, dbfc := range dbfs {
				hbf := dbf2hbf(familyname, idfieldname, dbfc)
				if hbf != nil {
					list.AddFilters(hbf)
				}
			}
			return list
		} else if dbf.Op == dbflex.OpEq {
			//fmt.Printf("search for %s = %s\n", dbf.Field, dbf.Value.(string))
			bdata := toBytes(reflect.ValueOf(dbf.Value), reflect.TypeOf(dbf.Value))
			hf := filter.NewSingleColumnValueFilter(
				[]byte(familyname), []byte(dbf.Field),
				filter.Equal,
				filter.NewBinaryComparator(filter.NewByteArrayComparable(bdata)),
				false, false)
			return hf
		} else if dbf.Op == dbflex.OpGt {
			bdata := toBytes(reflect.ValueOf(dbf.Value), reflect.TypeOf(dbf.Value))
			hf := filter.NewSingleColumnValueFilter(
				[]byte(familyname), []byte(dbf.Field),
				filter.Greater,
				filter.NewBinaryComparator(filter.NewByteArrayComparable(bdata)),
				false, false)
			return hf
		} else if dbf.Op == dbflex.OpGte {
			bdata := toBytes(reflect.ValueOf(dbf.Value), reflect.TypeOf(dbf.Value))
			hf := filter.NewSingleColumnValueFilter(
				[]byte(familyname), []byte(dbf.Field),
				filter.GreaterOrEqual,
				filter.NewBinaryComparator(filter.NewByteArrayComparable(bdata)),
				false, false)
			return hf
		} else if dbf.Op == dbflex.OpLt {
			bdata := toBytes(reflect.ValueOf(dbf.Value), reflect.TypeOf(dbf.Value))
			hf := filter.NewSingleColumnValueFilter(
				[]byte(familyname), []byte(dbf.Field),
				filter.Less,
				filter.NewBinaryComparator(filter.NewByteArrayComparable(bdata)),
				false, false)
			return hf
		} else if dbf.Op == dbflex.OpLte {
			bdata := toBytes(reflect.ValueOf(dbf.Value), reflect.TypeOf(dbf.Value))
			hf := filter.NewSingleColumnValueFilter(
				[]byte(familyname), []byte(dbf.Field),
				filter.LessOrEqual,
				filter.NewBinaryComparator(filter.NewByteArrayComparable(bdata)),
				false, false)
			return hf
		}
	}
	return nil
}

func (q *Query) BuildCommand() (interface{}, error) {
	return nil, nil
}

func toHbaseMutate(ctx context.Context, op string,
	table, key, idfieldname, family string, data interface{}) (*hrpc.Mutate, error) {
	hbaseData := map[string]map[string][]byte{}
	rv := reflect.Indirect(reflect.ValueOf(data))
	rt := rv.Type()
	if family == "" {
		family = DefaultFamilyName()
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
				t := elem.Interface().(*time.Time)
				i := t.Unix()
				familyData[fieldName] = int64ToByte(i)
			} else if elemTypeString == "time.Time" {
				t := elem.Interface().(time.Time)
				i := t.Unix()
				familyData[fieldName] = int64ToByte(i)
			} else if elemTypeString == "bool" {
				b := elem.Bool()
				if b {
					familyData[fieldName] = []byte{1}
				} else {
					familyData[fieldName] = []byte{0}
				}
			} else {
				familyData[fieldName] = []byte(elem.String())
				//familyData[fieldName] = []byte("Hello all")
			}
		}
	}
	hbaseData[family] = familyData

	//fmt.Println("data:", hbaseData)
	if op == "save" {
		putStr, err := hrpc.NewPutStr(ctx, table, key, hbaseData)
		return putStr, err
	} else if op == "delete" {
		req, err := hrpc.NewDelStr(ctx, table, key, hbaseData)
		return req, err
	} else {
		return nil, fmt.Errorf("invalid mutate operation. %s", op)
	}
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

func toBytes(v reflect.Value, t reflect.Type) []byte {
	tname := t.String()
	if tname != "interface{}" && strings.HasPrefix(tname, "int") {
		return int64ToByte(v.Int())
	} else if strings.HasPrefix(tname, "float") {
		return float64ToByte(v.Float())
	} else if tname == "*time.Time" {
		t := v.Interface().(time.Time)
		i := t.UnixNano()
		return int64ToByte(i)
	} else if tname == "time.Time" {
		t := v.Interface().(*time.Time)
		i := t.UnixNano()
		return int64ToByte(i)
	} else {
		return []byte(v.String())
		//familyData[fieldName] = []byte("Hello all")
	}
}
