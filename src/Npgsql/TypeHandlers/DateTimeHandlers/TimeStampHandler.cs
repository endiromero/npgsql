﻿using System;
using Npgsql.BackendMessages;
using NpgsqlTypes;
using System.Data;

namespace Npgsql.TypeHandlers.DateTimeHandlers
{
    /// <remarks>
    /// http://www.postgresql.org/docs/current/static/datatype-datetime.html
    /// </remarks>
    [TypeMapping("timestamp", NpgsqlDbType.Timestamp, new[] { DbType.DateTime, DbType.DateTime2 }, new [] { typeof(NpgsqlDateTime), typeof(DateTime) }, DbType.DateTime)]
    internal class TimeStampHandler : TypeHandlerWithPsv<DateTime, NpgsqlDateTime>,
        ISimpleTypeReader<DateTime>, ISimpleTypeReader<NpgsqlDateTime>, ISimpleTypeWriter
    {
        /// <summary>
        /// A deprecated compile-time option of PostgreSQL switches to a floating-point representation of some date/time
        /// fields. Npgsql (currently) does not support this mode.
        /// </summary>
        readonly bool _integerFormat;

        /// <summary>
        /// Whether to convert positive and negative infinity values to DateTime.{Max,Min}Value when
        /// a DateTime is requested
        /// </summary>
        readonly bool _convertInfinityDateTime;

        public TimeStampHandler(TypeHandlerRegistry registry)
        {
            _integerFormat = registry.Connector.BackendParams["integer_datetimes"] == "on";
            _convertInfinityDateTime = registry.Connector.ConvertInfinityDateTime;
        }

        public virtual DateTime Read(NpgsqlBuffer buf, int len, FieldDescription fieldDescription)
        {
            // TODO: Convert directly to DateTime without passing through NpgsqlTimeStamp?
            var ts = ReadTimeStamp(buf, len, fieldDescription);
            try
            {
                if (ts.IsFinite)
                    return ts.DateTime;
                if (!_convertInfinityDateTime)
                    throw new InvalidCastException("Can't convert infinite timestamp values to DateTime");
                if (ts.IsInfinity)
                    return DateTime.MaxValue;
                return DateTime.MinValue;
            }
            catch (Exception e)
            {
                throw new SafeReadException(e);
            }
        }

        NpgsqlDateTime ISimpleTypeReader<NpgsqlDateTime>.Read(NpgsqlBuffer buf, int len, FieldDescription fieldDescription)
        {
            return ReadTimeStamp(buf, len, fieldDescription);
        }

        protected NpgsqlDateTime ReadTimeStamp(NpgsqlBuffer buf, int len, FieldDescription fieldDescription)
        {
            if (!_integerFormat) {
                throw new NotSupportedException("Old floating point representation for timestamps not supported");
            }

            var value = buf.ReadInt64();
            if (value == long.MaxValue)
                return NpgsqlDateTime.Infinity;
            if (value == long.MinValue)
                return NpgsqlDateTime.NegativeInfinity;
            if (value >= 0) {
                int date = (int)(value / 86400000000L);
                long time = value % 86400000000L;

                date += 730119; // 730119 = days since era (0001-01-01) for 2000-01-01
                time *= 10; // To 100ns

                return new NpgsqlDateTime(new NpgsqlDate(date), new TimeSpan(time));
            } else {
                value = -value;
                int date = (int)(value / 86400000000L);
                long time = value % 86400000000L;
                if (time != 0) {
                    ++date;
                    time = 86400000000L - time;
                }
                date = 730119 - date; // 730119 = days since era (0001-01-01) for 2000-01-01
                time *= 10; // To 100ns

                return new NpgsqlDateTime(new NpgsqlDate(date), new TimeSpan(time));
            }
        }

        public int ValidateAndGetLength(object value)
        {
            return 8;
        }

        public virtual void Write(object value, NpgsqlBuffer buf)
        {
            NpgsqlDateTime ts;
            if (value is NpgsqlDateTime) {
                ts = (NpgsqlDateTime)value;
                if (!ts.IsFinite)
                {
                    if (ts.IsInfinity)
                    {
                        buf.WriteInt64(Int64.MaxValue);
                        return;
                    }

                    if (ts.IsNegativeInfinity)
                    {
                        buf.WriteInt64(Int64.MinValue);
                        return;
                    }

                    throw PGUtil.ThrowIfReached();
                }
            }
            else if (value is DateTime)
            {
                var dt = (DateTime)value;
                if (_convertInfinityDateTime)
                {
                    if (dt == DateTime.MaxValue)
                    {
                        buf.WriteInt64(Int64.MaxValue);
                        return;
                    }
                    else if (dt == DateTime.MinValue)
                    {
                        buf.WriteInt64(Int64.MinValue);
                        return;
                    }
                }
                ts = new NpgsqlDateTime(dt);
            }
            else if (value is DateTimeOffset)
            {
                ts = new NpgsqlDateTime(((DateTimeOffset)value).DateTime);
            }
            else if (value is string)
            {
                // TODO: Decide what to do with this
                throw new InvalidOperationException("String DateTimes are not allowed, use DateTime instead.");
            }
            else
            {
                throw new InvalidCastException();
            }

            var uSecsTime = ts.Time.Ticks / 10;

            if (ts >= new NpgsqlDateTime(2000, 1, 1, 0, 0, 0))
            {
                var uSecsDate = (ts.Date.DaysSinceEra - 730119) * 86400000000L;
                buf.WriteInt64(uSecsDate + uSecsTime);
            }
            else
            {
                var uSecsDate = (730119 - ts.Date.DaysSinceEra) * 86400000000L;
                buf.WriteInt64(-(uSecsDate - uSecsTime));
            }
        }
    }
}
