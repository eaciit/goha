package goha_test

import (
	"encoding/binary"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/goha"
	_ "github.com/eaciit/goha"
	"github.com/eaciit/toolkit"
	"github.com/sirupsen/logrus"

	cv "github.com/smartystreets/goconvey/convey"
)

type obj1 struct {
	ID     string
	Name   string
	Level  int
	Salary float64
}

type obj2 struct {
	ObjID       string
	Title       string
	DateCreated time.Time
	Enable      bool
	Salary      float64
}

var (
	connstr        = "hbase://localhost:2181/hbase"
	hbasenamespace = "bef"
	hbasefamily    = "def"

	t1 = "users"
	t2 = "obj2"
)

func connect() (dbflex.IConnection, error) {
	logrus.SetLevel(logrus.DebugLevel)
	goha.SetContextTimeOut(5 * time.Second)
	goha.SetActiveNameSpace(hbasenamespace)
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
		goha.SetDefaultFamilyName(hbasefamily)
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Insert data", func() {
			var es []string
			cmd := dbflex.From(t1).Save()

		insertLoop:
			for i := 0; i < 10; i++ {
				o := new(obj1)
				o.ID = toolkit.Sprintf("user-key-%d", i)
				o.Name = toolkit.Sprintf("name %d", i)
				o.Level = 110
				o.Salary = 1200.65
				_, e := c.Execute(cmd, toolkit.M{}.Set("data", o).
					Set("idfieldname", "ID"))
				if e != nil {
					es = append(es, toolkit.Sprintf("insert data %d error: %s", i, e.Error()))
					break insertLoop
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

func TestFilterIDFetchs(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Filter data", func() {
			cmd := dbflex.From(t1).Select().Where(
				dbflex.And(dbflex.Gte("ID", "user-key-2"), dbflex.Lte("ID", "user-key-4")))
			cs := c.Cursor(cmd, toolkit.M{}.Set("idfieldname", "ID"))
			cv.So(cs.Error(), cv.ShouldBeNil)

			objs := []obj1{}
			err := cs.Fetchs(&objs, 0)
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("Validate data", func() {
				cv.So(len(objs), cv.ShouldEqual, 3)
			})
		})
	})
}

func TestFilterValueFetchs(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Filter data", func() {
			var es []string
			cmd := dbflex.From(t1).Select().Where(dbflex.Eq("Name", "name 3"))
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
					if o.Name != "name 3" {
						es = append(es, toolkit.Sprintf("data %d not equal: %s != %s", i,
							o.Name, "name 3"))
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

func TestDelete(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Delete data", func() {
			//-- since delete by filter is not allowed on HBase, hence deley can only be done by key
			cmd := dbflex.From(t1).Delete()
			_, err := c.Execute(cmd, toolkit.M{}.Set("ID", []string{"user-key-3"}))
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("Validate data", func() {
				cmd := dbflex.From(t1).Select().Where(dbflex.Eq("ID", "user-key-3"))
				objs := []obj1{}
				cs := c.Cursor(cmd, nil)
				err := cs.Fetchs(&objs, 0)
				cv.So(err, cv.ShouldBeNil)
				cv.So(len(objs), cv.ShouldEqual, 0)
			})
		})
	})
}

func TestInsertObj2(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Insert data", func() {
			o := new(obj2)
			o.ObjID = "obj-id-1"
			o.Title = "This is objid"
			o.Enable = true
			o.DateCreated = time.Now()

			cmd := dbflex.From(t1).Save()
			_, err := c.Execute(cmd, toolkit.M{}.
				Set("data", o).
				Set("idfieldname", "ObjID"))
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("Get data", func() {
				cmd := dbflex.From(t1).Select().Where(dbflex.Eq("ObjID", "obj-id-1"))
				objs := []obj2{}
				cs := c.Cursor(cmd, toolkit.M{}.Set("idfieldname", "ObjID"))
				err := cs.Fetchs(&objs, 0)
				cv.So(err, cv.ShouldBeNil)
				cv.So(len(objs), cv.ShouldEqual, 1)

				cv.Convey("Validate data", func() {
					o2 := objs[0]
					cv.So(o2.ObjID, cv.ShouldEqual, o.ObjID)
					cv.So(o2.Title, cv.ShouldEqual, o.Title)

					diff := o.DateCreated.Sub(o2.DateCreated)
					cv.So(diff, cv.ShouldBeLessThan, time.Duration(1*time.Second))
					cv.So(o2.Enable, cv.ShouldEqual, o.Enable)
				})
			})
		})
	})
}

func TestDeleteObj2(t *testing.T) {
	cv.Convey("Prepare connection", t, func() {
		c, e := connect()
		cv.So(e, cv.ShouldBeNil)
		defer func() {
			c.Close()
		}()

		cv.Convey("Delete data", func() {
			//-- since delete by filter is not allowed on HBase, hence deley can only be done by key
			cmd := dbflex.From(t1).Delete()
			_, err := c.Execute(cmd, toolkit.M{}.
				Set("idfieldname", "ObjID").
				Set("ID", []string{"obj-id-1"}))
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("Validate data", func() {
				cmd := dbflex.From(t1).Select().Where(dbflex.Eq("ObjID", "obj-id-1"))
				objs := []obj1{}
				cs := c.Cursor(cmd,
					toolkit.M{}.Set("idfieldname", "ObjID"))
				err := cs.Fetchs(&objs, 0)
				cv.So(err, cv.ShouldBeNil)
				cv.So(len(objs), cv.ShouldEqual, 0)
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
