using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Npgsql.BackendMessages;
using NpgsqlTypes;

namespace Npgsql.TypeHandlers
{
    /// <summary>
    /// Type Handler for the postgis geometry type.
    /// </summary>
    [TypeMapping("geometry", NpgsqlDbType.Geometry, typeof(IGeometry))]
    class PostgisGeometryHandler : TypeHandler<IGeometry>
        , IChunkingTypeReader<IGeometry>, IChunkingTypeWriter
    {
        class Counter
        {
            private int _value = 0;

            public int Value
            {
                get { return _value; }
                set { _value = value; }
            }

            public void Increment()
            {
                _value++;
            }

            public static implicit operator int(Counter c)
            {
                return c._value;
            }
        }

        private uint? _srid;
        private uint _id = 0;

        private bool _newGeom;
        private int _ipol, _ipts, _irng;

        private NpgsqlBuffer _buf;
        private BBPoint[] _points;
        private BBPoint[][] _rings;
        private BBPoint[][][] _pols;
        private Stack<IGeometry[]> _geoms = new Stack<IGeometry[]>();
        private Stack<Counter> _icol = new Stack<Counter>();        
        private IGeometry _toWrite;


        public void PrepareRead(NpgsqlBuffer buf, int len, FieldDescription fieldDescription = null)
        {
            _buf = buf;
            _srid = default(uint?);
            _geoms.Clear();
            _icol.Clear();
            Reset();
        }

        private void Reset()
        {
            _points = null;
            _rings = null;
            _pols = null;
            _ipts = _irng = _ipol = -1;
            _newGeom = true;
            _id = 0;
        }

        public bool Read(out IGeometry result)
        {
            result = default(IGeometry);
            if (_id == 0)
            {
                if (_buf.ReadBytesLeft < 5)
                    return false;
                _buf.Skip(1); // byte storing the endianness of the data structure (canonical form = NDR).
                _id = _buf.PostgisReadUInt32();
            }
            if (!_srid.HasValue)
            {
                if ((_id & (uint)OgrModifier.HasSRID) != 0)
                {
                    if (_buf.ReadBytesLeft < 4)
                        return false;
                    _srid = _buf.PostgisReadUInt32();
                }
                else
                {
                    _srid = 0;
                }
            }

            switch ((OgrIdentifier)(_id & (uint)7))
            {
                case OgrIdentifier.Point:
                    if (_buf.ReadBytesLeft < 16)
                        return false;
                    result = new PostgisPoint(_buf.PostgisReadDouble(), _buf.PostgisReadDouble());
                    result.SRID = _srid.Value;
                    return true;

                case OgrIdentifier.LineString:
                    if (_ipts == -1)
                    {
                        if (_buf.ReadBytesLeft < 4)
                            return false;
                        _points = new BBPoint[_buf.PostgisReadInt32()];
                        _ipts = 0;
                    }
                    for (; _ipts < _points.Length; _ipts++)
                    {
                        if (_buf.ReadBytesLeft < 16)
                            return false;
                        _points[_ipts] = new BBPoint(_buf.PostgisReadDouble(), _buf.PostgisReadDouble());
                    }
                    result = new PostgisLineString(_points);
                    result.SRID = _srid.Value;
                    return true;

                case OgrIdentifier.Polygon:
                    if (_irng == -1)
                    {
                        if (_buf.ReadBytesLeft < 4)
                            return false;
                        _rings = new BBPoint[_buf.PostgisReadInt32()][];
                        _irng = 0;
                    }

                    for (; _irng < _rings.Length; _irng++)
                    {
                        if (_ipts == -1)
                        {
                            if (_buf.ReadBytesLeft < 4)
                                return false;
                            _rings[_irng] = new BBPoint[_buf.PostgisReadInt32()];
                            _ipts = 0;
                        }
                        for (; _ipts < _rings[_irng].Length; _ipts++)
                        {
                            if (_buf.ReadBytesLeft < 16)
                                return false;
                            _rings[_irng][_ipts] = new BBPoint(_buf.PostgisReadDouble(), _buf.PostgisReadDouble());
                        }
                        _ipts = -1;
                    }
                    result = new PostgisPolygon(_rings);
                    result.SRID = _srid.Value;
                    return true;

                case OgrIdentifier.MultiPoint:
                    if (_ipts == -1)
                    {
                        if (_buf.ReadBytesLeft < 4)
                            return false;
                        _points = new BBPoint[_buf.PostgisReadInt32()];
                        _ipts = 0;
                    }
                    for (; _ipts < _points.Length; _ipts++)
                    {
                        if (_buf.ReadBytesLeft < 21)
                            return false;
                        _buf.Skip(5);
                        _points[_ipts] = new BBPoint(_buf.PostgisReadDouble(), _buf.PostgisReadDouble());
                    }
                    result = new PostgisMultiPoint(_points);
                    result.SRID = _srid.Value;
                    return true;

                case OgrIdentifier.MultiLineString:
                    if (_irng == -1)
                    {
                        if (_buf.ReadBytesLeft < 4)
                            return false;
                        _rings = new BBPoint[_buf.PostgisReadInt32()][];
                        _irng = 0;
                    }

                    for (; _irng < _rings.Length; _irng++)
                    {
                        if (_ipts == -1)
                        {
                            if (_buf.ReadBytesLeft < 9)
                                return false;
                            _buf.Skip(5);
                            _rings[_irng] = new BBPoint[_buf.PostgisReadInt32()];
                            _ipts = 0;
                        }
                        for (; _ipts < _rings[_irng].Length; _ipts++)
                        {
                            if (_buf.ReadBytesLeft < 16)
                                return false;
                            _rings[_irng][_ipts] = new BBPoint(_buf.PostgisReadDouble(), _buf.PostgisReadDouble());
                        }
                        _ipts = -1;
                    }
                    result = new PostgisMultiLineString(_rings);
                    result.SRID = _srid.Value;
                    return true;

                case OgrIdentifier.MultiPolygon:
                    if (_ipol == -1)
                    {
                        if (_buf.ReadBytesLeft < 4)
                            return false;
                        _pols = new BBPoint[_buf.PostgisReadInt32()][][];
                        _ipol = 0;
                    }

                    for (; _ipol < _pols.Length; _ipol++)
                    {
                        if (_irng == -1)
                        {
                            if (_buf.ReadBytesLeft < 9)
                                return false;
                            _buf.Skip(5);
                            _pols[_ipol] = new BBPoint[_buf.PostgisReadInt32()][];
                            _irng = 0;
                        }
                        for (; _irng < _pols[_ipol].Length; _irng++)
                        {
                            if (_ipts == -1)
                            {
                                if (_buf.ReadBytesLeft < 4)
                                    return false;
                                _pols[_ipol][_irng] = new BBPoint[_buf.PostgisReadInt32()];
                                _ipts = 0;
                            }
                            for (; _ipts < _pols[_ipol][_irng].Length; _ipts++)
                            {
                                if (_buf.ReadBytesLeft < 16)
                                    return false;
                                _pols[_ipol][_irng][_ipts] = new BBPoint(_buf.PostgisReadDouble(), _buf.PostgisReadDouble());
                            }
                            _ipts = -1;
                        }
                        _irng = -1;
                    }
                    result = new PostgisMultiPolygon(_pols);
                    result.SRID = _srid.Value;
                    return true;

                case OgrIdentifier.GeometryCollection:
                    if (_newGeom)
                    {
                        if (_buf.ReadBytesLeft < 4)
                            return false;
                        _geoms.Push(new IGeometry[_buf.PostgisReadInt32()]);
                        _icol.Push(new Counter());
                    }
                    _id = 0;
                    var g = _geoms.Peek();
                    var i = _icol.Peek();
                    for (; i < g.Length; i.Increment())
                    {
                        IGeometry geom;
                        if (!Read(out geom))
                        {
                            _newGeom = false;
                            return false;
                        }
                        g[i] = geom;
                        Reset();
                    }
                    result = new PostgisGeometryCollection(g);
                    result.SRID = _srid.Value;
                    _geoms.Pop();
                    _icol.Pop();
                    return true;

                default:
                    throw new InvalidOperationException("Unknown Postgis identifier.");
            }
        }
        
        public int ValidateAndGetLength(object value, ref LengthCache lengthCache, NpgsqlParameter parameter = null)
        {
            var g = value as IGeometry;
            if (g == null)
                throw new InvalidCastException("IGeometry type expected.");
            return g.GetLen();
        }
        
        public void PrepareWrite(object value, NpgsqlBuffer buf, LengthCache lengthCache, NpgsqlParameter parameter = null)
        {
            _toWrite = value as IGeometry;
            if (_toWrite == null)
                throw new InvalidCastException("IGeometry type expected.");
            _buf = buf;
            _icol.Clear();
            Reset();
        }

        private bool Write(ref DirectBuffer directBuf,IGeometry geom)
        {
            if (_newGeom)
            {
                if (geom.SRID == 0)
                {
                    if (_buf.WriteSpaceLeft < 5)
                        return false;
                    _buf.WriteByte((byte)(BitConverter.IsLittleEndian ? 1 : 0));
                    _buf.PostgisWriteUInt32((uint)geom.Identifier);
                }
                else
                {
                    if (_buf.WriteSpaceLeft < 9)
                        return false;
                    _buf.WriteByte((byte)(BitConverter.IsLittleEndian ? 1 : 0));
                    _buf.PostgisWriteUInt32((uint)geom.Identifier | (uint) OgrModifier.HasSRID);
                    _buf.PostgisWriteUInt32(geom.SRID);
                }
                _newGeom = false;
            }
            switch (geom.Identifier)
            {
                case OgrIdentifier.Point:
                    if (_buf.WriteSpaceLeft < 16)
                        return false;
                    var p = (PostgisPoint)geom;
                    _buf.PostgisWriteDouble(p.X);
                    _buf.PostgisWriteDouble(p.Y);
                    return true;
                    
                case OgrIdentifier.LineString:
                    var l = (PostgisLineString)geom;
                    if (_ipts == -1)
                    {
                        if (_buf.WriteSpaceLeft < 4)
                            return false;
                        _buf.PostgisWriteUInt32((uint)l.PointCount);
                        _ipts = 0;
                    }
                    for (; _ipts < l.PointCount; _ipts++)
                    {
                        if (_buf.WriteSpaceLeft < 16)
                            return false;
                        _buf.PostgisWriteDouble(l[_ipts].X);
                        _buf.PostgisWriteDouble(l[_ipts].Y);
                    }
                    return true;

                case OgrIdentifier.Polygon:
                    var pol = (PostgisPolygon)geom;
                    if (_irng == -1)
                    {
                        if (_buf.WriteSpaceLeft < 4)
                            return false;
                        _buf.PostgisWriteUInt32((uint)pol.RingCount);
                        _irng = 0;
                    }
                    for (; _irng < pol.RingCount; _irng++)
                    {
                        if (_ipts == -1)
                        {
                            if (_buf.WriteSpaceLeft < 4)
                                return false;
                            _buf.PostgisWriteUInt32((uint)pol[_irng].Length);
                            _ipts = 0;
                        }
                        for (; _ipts < pol[_irng].Length; _ipts++)
                        {
                            if (_buf.WriteSpaceLeft < 16)
                                return false;
                            _buf.PostgisWriteDouble(pol[_irng][_ipts].X);
                            _buf.PostgisWriteDouble(pol[_irng][_ipts].Y);
                        }
                        _ipts = -1;
                    }
                    return true;

                case OgrIdentifier.MultiPoint:
                     var mp = (PostgisMultiPoint)geom;
                    if (_ipts == -1)
                    {
                        if (_buf.WriteSpaceLeft < 4)
                            return false;
                        _buf.PostgisWriteUInt32((uint)mp.PointCount);
                        _ipts = 0;
                    }
                    for (; _ipts < mp.PointCount; _ipts++)
                    {
                        if (_buf.WriteSpaceLeft < 21)
                            return false;
                        _buf.WriteByte((byte)(BitConverter.IsLittleEndian ? 1 : 0));
                        _buf.PostgisWriteUInt32((uint)OgrIdentifier.Point);
                        _buf.PostgisWriteDouble(mp[_ipts].X);
                        _buf.PostgisWriteDouble(mp[_ipts].Y);
                    }
                    return true;
                    
                case OgrIdentifier.MultiLineString:
                    var ml = (PostgisMultiLineString)geom;
                    if (_irng == -1)
                    {
                        if (_buf.WriteSpaceLeft < 4)
                            return false;
                        _buf.PostgisWriteInt32(ml.LineCount);
                        _irng = 0;
                    }
                    for (; _irng < ml.LineCount; _irng++)
                    {
                        if (_ipts == -1)
                        {
                            if (_buf.WriteSpaceLeft < 9)
                                return false;
                            _buf.WriteByte((byte)(BitConverter.IsLittleEndian ? 1 : 0));
                            _buf.PostgisWriteUInt32((uint)OgrIdentifier.LineString);
                            _buf.PostgisWriteUInt32((uint)ml[_irng].PointCount);
                            _ipts = 0;
                        }
                        for (; _ipts < ml[_irng].PointCount; _ipts++)
                        {
                            if (_buf.WriteSpaceLeft < 16)
                                return false;
                            _buf.PostgisWriteDouble(ml[_irng][_ipts].X);
                            _buf.PostgisWriteDouble(ml[_irng][_ipts].Y);
                        }
                        _ipts = -1;
                    }
                    return true;

                case OgrIdentifier.MultiPolygon:
                    var mpl = (PostgisMultiPolygon)geom;
                    if (_ipol == -1)
                    {
                        if (_buf.WriteSpaceLeft < 4)
                            return false;
                        _buf.PostgisWriteUInt32((uint)mpl.PolygonCount);
                        _ipol = 0;
                    }
                    for (; _ipol < mpl.PolygonCount; _ipol++)
                    {
                        if (_irng == -1)
                        {
                            if (_buf.WriteSpaceLeft < 9)
                                return false;
                            _buf.WriteByte((byte)(BitConverter.IsLittleEndian ? 1 : 0));
                            _buf.PostgisWriteUInt32((uint)OgrIdentifier.Polygon);
                            _buf.PostgisWriteUInt32((uint)mpl[_ipol].RingCount);
                            _irng = 0;
                        }
                        for (; _irng < mpl[_ipol].RingCount; _irng++)
                        {
                            if (_ipts == -1)
                            {
                                if (_buf.WriteSpaceLeft < 4)
                                    return false;
                                _buf.PostgisWriteUInt32((uint)mpl[_ipol][_irng].Length);
                                _ipts = 0;
                            }
                            for (; _ipts < mpl[_ipol][_irng].Length; _ipts++)
                            {
                                if (_buf.WriteSpaceLeft < 16)
                                    return false;
                                _buf.PostgisWriteDouble(mpl[_ipol][_irng][_ipts].X);
                                _buf.PostgisWriteDouble(mpl[_ipol][_irng][_ipts].Y);
                            }
                        }
                        _irng = -1;
                    }
                    return true;

                case OgrIdentifier.GeometryCollection:
                    var coll = (PostgisGeometryCollection)geom;
                    if (!_newGeom)
                    {
                        if (_buf.WriteSpaceLeft < 4)
                            return false;
                        _buf.PostgisWriteUInt32((uint)coll.GeometryCount);
                        _icol.Push(new Counter());
                        _newGeom = true;
                    }
                    for (Counter i = _icol.Peek(); i < coll.GeometryCount; i.Increment())
                    {
                        if (!Write(ref directBuf, coll[i]))
                            return false;
                        Reset();
                    }
                    _icol.Pop();
                    return true;

                default:
                    throw new InvalidOperationException("Unknown Postgis identifier.");
            }
        }

        public bool Write(ref DirectBuffer directBuf)
        {
            return Write(ref directBuf, _toWrite);
        }
    }
}