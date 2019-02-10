package goha

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/tsuna/gohbase/hrpc"
)

type Cursor struct {
	dbflex.CursorBase
	scanner hrpc.Scanner
}

func (cur *Cursor) Reset() error {
	return fmt.Errorf("reset function is not valid for hbase driver. Please initiate new cursor instead")
}

func (cur *Cursor) Fetch(dest interface{}) error {
	if cur.scanner == nil {
		return fmt.Errorf("cursor is not properly initiated. Scanner is missing")
	}
	res, err := cur.scanner.Next()
	if err == io.EOF {
		return err
	} else if err != nil {
		return fmt.Errorf("error fetching hbase cursor. %s", err.Error())
	}
	err = unmarshallData(res, dest, "ID")
	if err != nil {
		return fmt.Errorf("error decode hbase result. %s", err.Error())
	}
	return nil
}

func (cur *Cursor) Fetchs(dest interface{}, n int) error {
	if cur.scanner == nil {
		return fmt.Errorf("cursor is not properly initiated. Scanner is missing")
	}

	v := reflect.TypeOf(dest).Elem().Elem()
	ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

	read := 0
	for {
		res, err := cur.scanner.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("error fetching hbase cursor. %s", err.Error())
		}

		iv := reflect.New(v).Interface()
		err = unmarshallData(res, iv, "ID")
		if err != nil {
			return fmt.Errorf("error decode hbase result. %s", err.Error())
		}
		ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())

		read++
		if read == n {
			break
		}
	}
	reflect.ValueOf(dest).Elem().Set(ivs)

	return nil
}

func (cur *Cursor) Count() int {
	return 0
}

func (cur *Cursor) Close() {
	if cur.scanner != nil {
		cur.scanner.Close()
	}
	cur.scanner = nil
}

func unmarshallData(res *hrpc.Result, dest interface{}, idFieldName string) error {
	rv := reflect.ValueOf(dest).Elem()
	rt := rv.Type()

	if idFieldName != "" {
		idField := rv.FieldByName(idFieldName)
		if idField.IsValid() {
			idField.SetString(string(res.Cells[0].Row))
		}
	}

	for _, cell := range res.Cells {
		name := string(cell.Qualifier)
		bytevalue := cell.Value

		if tField, ok := rt.FieldByName(name); ok {
			vField := rv.FieldByName(name)
			tname := tField.Type.String()

			if strings.HasPrefix(tname, "int") && tname != "interface{}" {
				vField.SetInt(byteToInt(bytevalue))
			} else if strings.HasPrefix(tname, "float") {
				vField.SetFloat(byteToFloat64(bytevalue))
				//fmt.Printf("salary:%f\n", byteToFloat64(bytevalue))
			} else if tname == "string" {
				vField.SetString(string(bytevalue))
			} else {
				vField.Set(reflect.ValueOf(bytevalue))
			}
		}
	}

	//fmt.Printf("data: %v\n", toolkit.JsonString(rv.Interface()))
	return nil
}

func byteToInt(bs []byte) int64 {
	return int64(binary.BigEndian.Uint64(bs))
}

func byteToFloat64(bs []byte) float64 {
	bits := binary.LittleEndian.Uint64(bs)
	float := math.Float64frombits(bits)
	return float
}
