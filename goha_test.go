package goha_test

import (
	"encoding/binary"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"git.eaciitapp.com/sebar/dbflex"
	_ "github.com/eaciit/goha"
	"github.com/eaciit/toolkit"

	cv "github.com/smartystreets/goconvey/convey"
)

type obj1 struct {
	ID     string
	Name   string
	Level  int
	Salary float64
}

type obj2 struct {
	ID          string
	Title       string
	DateCreated time.Time
}

var (
	connstr        = "hbase://localhost:2181"
	hbasenamespace = "bef"
	hbasefamily    = "def"

	t1 = "users"
	t2 = "obj2"
)

func connect() (dbflex.IConnection, error) {
	c, err := dbflex.NewConnectionFromURI(connstr, nil)
	if err != nil {
		return nil, err
	}

	err = c.Connect()
	return c, err
}

func TestConnect(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, err := connect()
		cv.So(err, cv.ShouldBeNil)

		defer c.Close()
	})
}

func TestDeleteTable(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Delete Table", func() {
			cmd := dbflex.From(t1).Command("delete-table")
			_, e := c.Execute(cmd, nil)
			cv.So(e, cv.ShouldBeNil)
		})
	})
}

func TestCreateTable(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Create Table", func() {
			cmd := dbflex.From(t1).Command("create-table")
			_, e := c.Execute(cmd, toolkit.M{}.Set("family", hbasefamily))
			cv.So(e, cv.ShouldBeNil)
		})
	})
}

func TestInsertData(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Insert data", func() {
			var es []string
			cmd := dbflex.From(t1).Insert()
			for i := 0; i < 10; i++ {
				o := new(obj1)
				o.ID = toolkit.Sprintf("user-key-%d", i)
				o.Name = toolkit.Sprintf("user %d", i)
				o.Level = 110
				o.Salary = 1200.65
				_, e := c.Execute(cmd, toolkit.M{}.Set("data", o).
					Set("family", hbasefamily).
					Set("idfieldname", "ID"))
				if e != nil {
					es = append(es, toolkit.Sprintf("insert data %d error: %s", i, e.Error()))
					break
				}
			}

			esall := ""
			if len(es) > 0 {
				esall = strings.Join(es, "\n")
			}
			cv.So(esall, cv.ShouldEqual, "")
		})
	})
}

func TestReadAllDataFetch(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Fetch data", func() {
			var es []string
			cmd := dbflex.From(t1).Select()
			cs := c.Cursor(cmd, nil)
			cv.So(cs.Error(), cv.ShouldBeNil)

			esall := ""
			i := 0
			for {
				o := new(obj1)
				e = cs.Fetch(o)
				if e == io.EOF {
					break
				} else if e != nil {
					es = append(es, toolkit.Sprintf("fetch data %d error: %s", i, e.Error()))
					//break
				}

				if o.ID != toolkit.Sprintf("user-key-%d", i) {
					es = append(es, toolkit.Sprintf("data %d not equal: %s != %s", i,
						o.ID, toolkit.Sprintf("user-key-%d", i)))
				}
				i++
			}

			if len(es) > 0 {
				esall = strings.Join(es, "\n")
			}
			cv.So(esall, cv.ShouldEqual, "")
		})
	})
}

func TestReadAllDataFetchs(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Fetchs data", func() {
			var es []string
			cmd := dbflex.From(t1).Select()
			cs := c.Cursor(cmd, nil)
			cv.So(cs.Error(), cv.ShouldBeNil)

			objs := []obj1{}
			err := cs.Fetchs(&objs, 0)
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("Validate data", func() {
				cv.So(len(objs), cv.ShouldEqual, 10)

				esall := ""
				i := 0
				for _, o := range objs {
					if o.ID != toolkit.Sprintf("user-key-%d", i) {
						es = append(es, toolkit.Sprintf("data %d not equal: %s != %s", i,
							o.ID, toolkit.Sprintf("user-key-%d", i)))
					}
					i++
				}

				if len(es) > 0 {
					esall = strings.Join(es, "\n")
				}
				cv.So(esall, cv.ShouldEqual, "")
			})
		})
	})
}

func TestFilterFetchs(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Filter data", func() {
			var es []string
			cmd := dbflex.From(t1).Select().Where(dbflex.Eq("Name", "user 3"))
			cs := c.Cursor(cmd, nil)
			cv.So(cs.Error(), cv.ShouldBeNil)

			objs := []obj1{}
			err := cs.Fetchs(&objs, 0)
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("Validate data", func() {
				cv.So(len(objs), cv.ShouldEqual, 1)

				esall := ""
				i := 0
				for _, o := range objs {
					if o.ID != toolkit.Sprintf("user-key-%d", i) {
						es = append(es, toolkit.Sprintf("data %d not equal: %s != %s", i,
							o.ID, toolkit.Sprintf("user-key-%d", i)))
					}
					i++
				}

				if len(es) > 0 {
					esall = strings.Join(es, "\n")
				}
				cv.So(esall, cv.ShouldEqual, "")
			})
		})
	})
}

func TestEncodingFloat(t *testing.T) {
	cv.Convey("Float", t, func() {
		origin := float64(10231.87)
		bs := float64ToByte(origin)
		res := byteToFloat64(bs)
		cv.So(res, cv.ShouldEqual, origin)
	})
}

func float64ToByte(f float64) []byte {
	bits := math.Float64bits(f)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

func byteToFloat64(bs []byte) float64 {
	bits := binary.LittleEndian.Uint64(bs)
	float := math.Float64frombits(bits)
	return float
}
