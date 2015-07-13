using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Npgsql;
using NpgsqlTypes;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace Npgsql.Tests.Types
{
    class PostgisTest : TestBase
    {
        class TestAtt
        {
            public IGeometry Geom;
            public string SQL;
        }

        private Dictionary<string, TestAtt> _geoms = new Dictionary<string, TestAtt>();
        
        [Test]
        [TestCase("point")]
        [TestCase("line")]
        [TestCase("poly")]
        [TestCase("mpoint")]
        [TestCase("mline")]
        [TestCase("mpoly")]
        [TestCase("coll")]
        [TestCase("nestedcoll")]
        public void PostgisTestRead(string geomIdx)
        {
            using (var cmd = Conn.CreateCommand())
            {
                var a = _geoms[geomIdx];
                cmd.CommandText = "Select " + a.SQL;
                var p = cmd.ExecuteScalar();
                Assert.IsTrue(p.Equals(a.Geom));
            }
        }

        [Test]
        [TestCase("point")]
        [TestCase("line")]
        [TestCase("poly")]
        [TestCase("mpoint")]
        [TestCase("mline")]
        [TestCase("mpoly")]
        [TestCase("coll")]
        [TestCase("nestedcoll")]
        public void PostgisTestWrite(string geomIdx)
        {
            using (var cmd = Conn.CreateCommand())
            {                
                cmd.Parameters.AddWithValue("p1", NpgsqlTypes.NpgsqlDbType.Geometry,_geoms[geomIdx].Geom);
                _geoms[geomIdx].Geom.SRID = 0;
                cmd.CommandText = "Select st_asewkb(:p1) = st_asewkb(" + _geoms[geomIdx].SQL + ")";
                Assert.IsTrue((bool)cmd.ExecuteScalar());
            }
        }

        [Test]
        [TestCase("point")]
        [TestCase("line")]
        [TestCase("poly")]
        [TestCase("mpoint")]
        [TestCase("mline")]
        [TestCase("mpoly")]
        [TestCase("coll")]
        [TestCase("nestedcoll")]
        public void PostgisTestWriteSrid(string geomIdx)
        {
            using (var cmd = Conn.CreateCommand())
            {
                cmd.Parameters.AddWithValue("p1", NpgsqlTypes.NpgsqlDbType.Geometry, _geoms[geomIdx].Geom);
                _geoms[geomIdx].Geom.SRID = 3942;
                cmd.CommandText = "Select st_asewkb(:p1) = st_asewkb(st_setsrid("+ _geoms[geomIdx].SQL + ",3942))";
                var p = (bool)cmd.ExecuteScalar();
                Assert.IsTrue(p);
            }
        }

        [Test]
        [TestCase("point")]
        [TestCase("line")]
        [TestCase("poly")]
        [TestCase("mpoint")]
        [TestCase("mline")]
        [TestCase("mpoly")]
        [TestCase("coll")]
        [TestCase("nestedcoll")]
        public void PostgisTestReadSrid(string geomIdx)
        {
            using (var cmd = Conn.CreateCommand())
            {
                var a = _geoms[geomIdx];
                cmd.CommandText = "Select st_setsrid(" + a.SQL + ",3942)";
                var p = cmd.ExecuteScalar();
                Assert.IsTrue(p.Equals(a.Geom));
                Assert.IsTrue((p as IGeometry).SRID == 3942);
            }
        }
        
        protected override void SetUp()
        {
            base.SetUp();
            using (var cmd = Conn.CreateCommand())
            {
                cmd.CommandText = "SELECT postgis_version();";
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (NpgsqlException)
                {
                    Assert.Ignore("Skipping tests : postgis extension not found.");
                }
            }
        }

        [Test]
        public void PostgisTestArrayRead()
        {
            using (var cmd = Conn.CreateCommand())
            {                
                cmd.CommandText = "Select ARRAY(select st_makepoint(1,1))";
                var p = cmd.ExecuteScalar() as IGeometry[];
                var p2 = new PostgisPoint(1d,1d);
                Assert.IsTrue(p != null &&  p[0] is PostgisPoint && p2 == (PostgisPoint)p[0]);
            }
        }

        [Test]
        public void PostgisTestArrayWrite()
        {
            using (var cmd = Conn.CreateCommand())
            {
                var p = new PostgisPoint[1] { new PostgisPoint(1d, 1d) };
                cmd.Parameters.AddWithValue(":p1", NpgsqlDbType.Array | NpgsqlDbType.Geometry, p);
                cmd.CommandText = "SELECT :p1 = array(select st_makepoint(1,1))";
                Assert.IsTrue((bool)cmd.ExecuteScalar());
            }
        }

        public PostgisTest(string backendversion) : base(backendversion)
        {
            #region Dummy geom init
            _geoms.Add("point", new TestAtt() { Geom = new PostgisPoint(1D, 2500D), SQL = "st_makepoint(1,2500)" });

            _geoms.Add("line", new TestAtt()
            {
                Geom = new PostgisLineString(new BBPoint[] { new BBPoint(1D, 1D), new BBPoint(1D, 2500D) }),
                SQL = "st_makeline(st_makepoint(1,1),st_makepoint(1,2500))"
            });

            _geoms.Add("poly", new TestAtt()
            {
                Geom = new PostgisPolygon(new BBPoint[][] 
                                                    { new BBPoint[] {
                                                        new BBPoint(1d,1d),
                                                        new BBPoint(2d,2d),
                                                        new BBPoint(3d,3d),
                                                        new BBPoint(1d,1d)}
                                                    }),
                SQL = "st_makepolygon(st_makeline(ARRAY[st_makepoint(1,1),st_makepoint(2,2),st_makepoint(3,3),st_makepoint(1,1)]))"
            });

            _geoms.Add("mpoint", new TestAtt()
            {
                Geom = new PostgisMultiPoint(new BBPoint[] { new BBPoint(1D, 1D) }),
                SQL = "st_multi(st_makepoint(1,1))"
            });

            _geoms.Add("mline", new TestAtt()
            {
                Geom = new PostgisMultiLineString(new PostgisLineString[] { new PostgisLineString(new BBPoint[] { new BBPoint(1D, 1D), new BBPoint(1D, 2500D) }) }),
                SQL = "st_multi(st_makeline(st_makepoint(1,1),st_makepoint(1,2500)))"
            });

            _geoms.Add("mpoly", new TestAtt()
            {
                Geom = new PostgisMultiPolygon(new PostgisPolygon[] {new PostgisPolygon( new BBPoint[][] 
                                                    { new BBPoint[] {
                                                        new BBPoint(1d,1d),
                                                        new BBPoint(2d,2d),
                                                        new BBPoint(3d,3d),
                                                        new BBPoint(1d,1d)}
                                                    }) }),
                SQL = "st_multi(st_makepolygon(st_makeline(ARRAY[st_makepoint(1,1),st_makepoint(2,2),st_makepoint(3,3),st_makepoint(1,1)])))"
            });

            _geoms.Add("coll", new TestAtt()
                {
                    Geom = new PostgisGeometryCollection(new IGeometry[]{
                            new PostgisPoint(1,1),
                            new PostgisMultiPolygon (new PostgisPolygon[] {new PostgisPolygon( new BBPoint[][] 
                                                    { new BBPoint[] {
                                                        new BBPoint(1d,1d),
                                                        new BBPoint(2d,2d),
                                                        new BBPoint(3d,3d),
                                                        new BBPoint(1d,1d)}
                                                    })})
                            }
                            ),
                    SQL = "ST_ForceCollection(st_collect(st_makepoint(1,1),st_multi(st_makepolygon(st_makeline(ARRAY[st_makepoint(1,1),st_makepoint(2,2),st_makepoint(3,3),st_makepoint(1,1)])))))"
                });

            _geoms.Add("nestedcoll", new TestAtt()
            {
                Geom =      new PostgisGeometryCollection(new IGeometry[]
                            {
                                new PostgisPoint(1,1),
                                new PostgisGeometryCollection(new IGeometry[]
                                {
                                    new PostgisPoint(1,1),
                                    new PostgisMultiPolygon (new PostgisPolygon[] {
                                                                                new PostgisPolygon( new BBPoint[][] 
                                                                                                  { new BBPoint[] {
                                                                                                    new BBPoint(1d,1d),
                                                                                                    new BBPoint(2d,2d),
                                                                                                    new BBPoint(3d,3d),
                                                                                                    new BBPoint(1d,1d)}
                                                                                                  })
                                                             })
                                })
                            }),
                SQL = "st_forcecollection(st_collect(st_makepoint(1,1),ST_ForceCollection(st_collect(st_makepoint(1,1),st_multi(st_makepolygon(st_makeline(ARRAY[st_makepoint(1,1),st_makepoint(2,2),st_makepoint(3,3),st_makepoint(1,1)])))))))"
            });
            #endregion
        }
    }
}