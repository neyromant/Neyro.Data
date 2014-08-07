using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace Neyro.Data
{
    public class DataManager : IDisposable
    {
        private delegate T DmDelegate<T>(IDataRecord dr);

        private static readonly Dictionary<Type, Dictionary<int, Delegate>> invokes = new Dictionary<Type, Dictionary<int, Delegate>>();
        private static readonly object invokeLock = new object();
        private static readonly MethodInfo getValueMethod = typeof(IDataRecord).GetMethod("get_Item", new Type[] { typeof(int) });
        private static readonly MethodInfo isDBNullMethod = typeof(IDataRecord).GetMethod("IsDBNull", new Type[] { typeof(int) });

        private IDbConnection connection;
        private IDbCommand command;
        private int cmdHashCode;
        internal IDbCommand Command { get { return this.command; } }

        public DataManager(IDbConnection conn)
        {
            this.connection = conn;
        }

        public DataManager Procedure(string cmdText)
        {
            this.command = this.connection.CreateCommand();
            this.command.CommandText = cmdText;
            this.command.CommandType = CommandType.StoredProcedure;
            this.cmdHashCode = cmdText.GetHashCode();
            return this;
        }

        public DataManager Sql(string cmdText)
        {
            this.command = this.connection.CreateCommand();
            this.command.CommandText = cmdText;
            this.command.CommandType = CommandType.Text;
            this.cmdHashCode = cmdText.GetHashCode();
            return this;
        }

        public DataManager AddParams(object @params)
        {
            var type = @params.GetType();
            var props = type.GetProperties();

            foreach (var p in props)
            {
                var newPar = this.command.CreateParameter();
                newPar.ParameterName = String.Concat("@", p.Name);
                newPar.Value = p.GetValue(@params, null) ?? DBNull.Value;
                this.command.Parameters.Add(newPar);
            }

            return this;
        }

        public DataManager AddEnumerableParam<T>(string paramName, IEnumerable<T> paramValue)
        {
            var ttype = typeof(T);
            var props = ttype.GetProperties();

            var table = new DataTable();

            table.Columns.AddRange(props.Select(p =>
                p.PropertyType.IsGenericType && p.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>)
                ? new DataColumn(p.Name, p.PropertyType.GetGenericArguments()[0]) { AllowDBNull = true }
                : new DataColumn(p.Name, p.PropertyType)
            ).ToArray());

            foreach (var d in paramValue)
            {
                var row = table.NewRow();
                foreach (var p in props)
                {
                    var v = p.GetValue(d, null);
                    if (v == null)
                        row[p.Name] = DBNull.Value;
                    else
                        row[p.Name] = v;
                }
                table.Rows.Add(row);
            }
            this.command.Parameters.Add(new System.Data.SqlClient.SqlParameter(string.Format("@{0}", paramName), table));
            return this;
        }

        public T Scalar<T>()
        {
            this.OpenConnection();
            T result = default(T);
            var r = this.command.ExecuteScalar();
            if (r != null) result = (T)r;
            return result;
        }

        public int Execute()
        {
            this.OpenConnection();
            return this.command.ExecuteNonQuery();
        }

        #region Get
        public T Get<T>(params Action<IDataRecord, T>[] detailCreators) 
        {
            this.OpenConnection();
            using (var dr = this.command.ExecuteReader())
            {
                T res = dr.Read() ? this.Create<T>(dr) : default(T);
                if (detailCreators != null && detailCreators.Length > 0)
                    foreach (var detailCreator in detailCreators)
                        if (dr.NextResult())
                            while (dr.Read())
                                detailCreator(dr, res);
                return res;
            }
        }

        
        public T Get<T, TA>(Func<T, List<TA>> subList)
            where T : new()
            where TA : new()
        {
            this.OpenConnection();
            using (var dr = this.command.ExecuteReader())
            {
                T res = dr.Read() ? this.Create<T>(dr) : default(T);
                if (dr.NextResult())
                {
                    var creator = this.GetCreator<TA>(dr);
                    while (dr.Read())
                        subList(res).Add(creator(dr));
                }
                return res;
            }
        }

        public T Get<T, TA, TB>(Func<T, List<TA>> subList, Func<T, List<TB>> subList2)
            where T : new()
            where TA : new()
            where TB : new()
        {
            this.OpenConnection();
            using (var dr = this.command.ExecuteReader())
            {
                T res = dr.Read() ? this.Create<T>(dr) : default(T);
                if (dr.NextResult())
                {
                    var creatorA = this.GetCreator<TA>(dr);
                    while (dr.Read())
                        subList(res).Add(creatorA(dr));

                    if (dr.NextResult())
                    {
                        var creatorB = this.GetCreator<TB>(dr);
                        while (dr.Read())
                            subList2(res).Add(creatorB(dr));
                    }
                }
                return res;
            }
        }

        public T Get<T, TA, TB, TC>(Func<T, List<TA>> subList, Func<T, List<TB>> subList2, Func<T, List<TC>> subList3)
            where T : new()
            where TA : new()
            where TB : new()
            where TC : new()
        {
            this.OpenConnection();
            using (var dr = this.command.ExecuteReader())
            {
                T res = dr.Read() ? this.Create<T>(dr) : default(T);
                if (dr.NextResult())
                {
                    var creatorA = this.GetCreator<TA>(dr);
                    while (dr.Read())
                        subList(res).Add(creatorA(dr));

                    if (dr.NextResult())
                    {
                        var creatorB = this.GetCreator<TB>(dr);
                        while (dr.Read())
                            subList2(res).Add(creatorB(dr));

                        if (dr.NextResult())
                        {
                            var creatorC = this.GetCreator<TC>(dr);
                            while (dr.Read())
                                subList3(res).Add(creatorC(dr));
                        }
                    }
                }
                return res;
            }
        }

        public T Get<T, TA, TB, TC, TD>(Func<T, List<TA>> subList, Func<T, List<TB>> subList2, Func<T, List<TC>> subList3, Func<T, List<TD>> subList4)
            where T : new()
            where TA : new()
            where TB : new()
            where TC : new()
            where TD : new()
        {
            this.OpenConnection();
            using (var dr = this.command.ExecuteReader())
            {
                var res = dr.Read() ? this.Create<T>(dr) : default(T);
                if (dr.NextResult())
                {
                    var creatorA = this.GetCreator<TA>(dr);
                    while (dr.Read())
                        subList(res).Add(creatorA(dr));

                    if (dr.NextResult())
                    {
                        var creatorB = this.GetCreator<TB>(dr);
                        while (dr.Read())
                            subList2(res).Add(creatorB(dr));

                        if (dr.NextResult())
                        {
                            var creatorC = this.GetCreator<TC>(dr);
                            while (dr.Read())
                                subList3(res).Add(creatorC(dr));

                            if (dr.NextResult())
                            {
                                var creatorD = this.GetCreator<TD>(dr);
                                while (dr.Read())
                                    subList4(res).Add(creatorD(dr));
                            }
                        }
                    }
                }
                return res;
            }
        }
        #endregion

        #region GetList
        public List<T> GetList<T>(params Action<IDataRecord, List<T>>[] detailCreators)
        {
            this.OpenConnection();
            var res = new List<T>();
            using (var dr = this.command.ExecuteReader())
            {
                var creator = this.GetCreator<T>(dr);
                while (dr.Read()) res.Add(creator(dr));

                if (detailCreators != null && detailCreators.Length > 0)
                    foreach (var detailCreator in detailCreators)
                        if (dr.NextResult())
                            while (dr.Read())
                                detailCreator(dr, res);
            }
            return res;
        }

        public List<T> GetList<T, TA>(Func<IEnumerable<T>, TA, List<TA>> sub)
            where T : new()
            where TA : new()
        {
            this.OpenConnection();
            var res = new List<T>();
            using (var dr = this.command.ExecuteReader())
            {
                var creator = this.GetCreator<T>(dr);
                while (dr.Read()) res.Add(creator(dr));
                if (dr.NextResult())
                {
                    var creatorA = this.GetCreator<TA>(dr);
                    while (dr.Read())
                    {
                        var a = creatorA(dr);
                        sub(res, a).Add(a);
                    }
                }
            }
            return res;
        }

        public List<T> GetList<T, TA, TB>(Func<IEnumerable<T>, TA, List<TA>> subA, Func<IEnumerable<T>, TB, List<TB>> subB)
            where T : new()
            where TA : new()
            where TB : new()
        {
            this.OpenConnection();
            var res = new List<T>();
            using (var dr = this.command.ExecuteReader())
            {
                var creator = this.GetCreator<T>(dr);
                while (dr.Read()) res.Add(creator(dr));
                if (dr.NextResult())
                {
                    var creatorA = this.GetCreator<TA>(dr);
                    while (dr.Read())
                    {
                        var a = creatorA(dr);
                        subA(res, a).Add(a);
                    }

                    if (dr.NextResult())
                    {
                        var creatorB = this.GetCreator<TB>(dr);
                        while (dr.Read())
                        {
                            var b = creatorB(dr);
                            subB(res, b).Add(b);
                        }
                    }
                }
            }
            return res;
        }

        public List<T> GetList<T, TA, TB, TC>(Func<IEnumerable<T>, TA, List<TA>> subA, Func<IEnumerable<T>, TB, List<TB>> subB, Func<IEnumerable<T>, TC, List<TC>> subC)
            where T : new()
            where TA : new()
            where TB : new()
            where TC : new()
        {
            this.OpenConnection();
            var res = new List<T>();
            using (var dr = this.command.ExecuteReader())
            {
                var creator = this.GetCreator<T>(dr);
                while (dr.Read()) res.Add(creator(dr));
                if (dr.NextResult())
                {
                    var creatorA = this.GetCreator<TA>(dr);
                    while (dr.Read())
                    {
                        var a = creatorA(dr);
                        subA(res, a).Add(a);
                    }

                    if (dr.NextResult())
                    {
                        var creatorB = this.GetCreator<TB>(dr);
                        while (dr.Read())
                        {
                            var b = creatorB(dr);
                            subB(res, b).Add(b);
                        }

                        if (dr.NextResult())
                        {
                            var creatorC = this.GetCreator<TC>(dr);
                            while (dr.Read())
                            {
                                var c = creatorC(dr);
                                subC(res, c).Add(c);
                            }
                        }
                    }
                }
            }
            return res;
        }

        public List<T> GetList<T, TA, TB, TC, TD>(Func<IEnumerable<T>, TA, List<TA>> subA, Func<IEnumerable<T>, TB, List<TB>> subB, Func<IEnumerable<T>, TC, List<TC>> subC, Func<IEnumerable<T>, TD, List<TD>> subD)
            where T : new()
            where TA : new()
            where TB : new()
            where TC : new()
            where TD : new()
        {
            this.OpenConnection();
            var res = new List<T>();
            using (var dr = this.command.ExecuteReader())
            {
                var creator = this.GetCreator<T>(dr);
                while (dr.Read()) res.Add(creator(dr));
                if (dr.NextResult())
                {
                    var creatorA = this.GetCreator<TA>(dr);
                    while (dr.Read())
                    {
                        var a = creatorA(dr);
                        subA(res, a).Add(a);
                    }

                    if (dr.NextResult())
                    {
                        var creatorB = this.GetCreator<TB>(dr);
                        while (dr.Read())
                        {
                            var b = creatorB(dr);
                            subB(res, b).Add(b);
                        }

                        if (dr.NextResult())
                        {
                            var creatorC = this.GetCreator<TC>(dr);
                            while (dr.Read())
                            {
                                var c = creatorC(dr);
                                subC(res, c).Add(c);
                            }

                            if (dr.NextResult())
                            {
                                var creatorD = this.GetCreator<TD>(dr);
                                while (dr.Read())
                                {
                                    var d = creatorD(dr);
                                    subD(res, d).Add(d);
                                }
                            }
                        }
                    }
                }
            }
            return res;
        }
        #endregion



        public void Raw(Action<IDataReader> action)
        {
            this.OpenConnection();
            using (var dr = this.command.ExecuteReader())
            {
                action(dr);
            }
        }

        public T Create<T>(IDataRecord dr)
        {
            return this.GetCreator<T>(dr)(dr);
        }

        private DmDelegate<T> GetCreator<T>(IDataRecord dr) 
        {
            var ct = typeof(T);
            if (!invokes.ContainsKey(ct) || !invokes[ct].ContainsKey(cmdHashCode))
            {
                lock (invokeLock)
                {
                    if (!invokes.ContainsKey(ct) || !invokes[ct].ContainsKey(cmdHashCode))
                    {
                        Type[] methodArgs2 = { typeof(IDataRecord) };
                        DynamicMethod method = new DynamicMethod(
                            "ct",
                            ct,
                            methodArgs2, typeof(DataManager));

                        ILGenerator generator = method.GetILGenerator();
                        LocalBuilder localV = generator.DeclareLocal(ct);

                        if (CheckIsPrimitiveType(ct))
                        {
                            generator.Emit(OpCodes.Ldarg_0);
                            generator.Emit(OpCodes.Ldc_I4, 0);
                            generator.Emit(OpCodes.Call, getValueMethod);
                            generator.Emit(OpCodes.Unbox_Any, dr.GetFieldType(0));
                            generator.Emit(OpCodes.Stloc, localV);
                        }
                        else
                        {
                            generator.Emit(OpCodes.Newobj, ct.GetConstructor(Type.EmptyTypes));
                            generator.Emit(OpCodes.Stloc, localV);
                            for (int i = 0; i < dr.FieldCount; i++)
                            {
                                var propertyInfo = ct.GetProperty(dr.GetName(i));

                                if (propertyInfo != null)
                                {
                                    var setMethod = propertyInfo.GetSetMethod();
                                    if (setMethod != null)
                                    {
                                        var endIfLabel = generator.DefineLabel();

                                        generator.Emit(OpCodes.Ldarg_0);
                                        generator.Emit(OpCodes.Ldc_I4, i);
                                        generator.Emit(OpCodes.Callvirt, isDBNullMethod);
                                        generator.Emit(OpCodes.Brtrue, endIfLabel);

                                        generator.Emit(OpCodes.Ldloc, localV);
                                        generator.Emit(OpCodes.Ldarg_0);
                                        generator.Emit(OpCodes.Ldc_I4, i);
                                        generator.Emit(OpCodes.Call, getValueMethod);

                                        generator.Emit(OpCodes.Unbox_Any, dr.GetFieldType(i));
                                        if (propertyInfo.PropertyType.IsGenericType && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                                        {
                                            generator.Emit(OpCodes.Newobj, propertyInfo.PropertyType.GetConstructor(propertyInfo.PropertyType.GetGenericArguments()));
                                        }
                                        generator.Emit(OpCodes.Call, setMethod);

                                        generator.MarkLabel(endIfLabel);
                                    }
                                }
                                else
                                {
                                    var name = dr.GetName(i);
                                    this.WorkWithSubClass(generator, localV, ct, name, dr, i);
                                }
                            }
                        }
                        generator.Emit(OpCodes.Ldloc, localV);
                        generator.Emit(OpCodes.Ret);
                        if (!invokes.ContainsKey(ct)) invokes.Add(ct, new Dictionary<int, Delegate>());
                        var del = (DmDelegate<T>)method.CreateDelegate(typeof(DmDelegate<T>));
                        invokes[ct].Add(cmdHashCode, del);
                    }
                }
            }
            return invokes[ct][cmdHashCode] as DmDelegate<T>;
        }

        private bool WorkWithSubClass(ILGenerator generator, LocalBuilder localV, Type type, string name, IDataRecord dr, int i)
        {
            var props = type.GetProperties().Where(p => name.IndexOf(p.Name) == 0);
            foreach (var p in props)
            {
                var fname = name.Remove(0, p.Name.Length);
                if (string.IsNullOrEmpty(fname)) break;
                var getMethod = p.GetGetMethod();

                if (getMethod != null)
                {
                    var subPropertyInfo = p.PropertyType.GetProperty(fname);
                    if (subPropertyInfo != null)
                    {
                        var setMethod = subPropertyInfo.GetSetMethod();
                        if (setMethod != null)
                        {
                            var endIfLabel = generator.DefineLabel();

                            generator.Emit(OpCodes.Ldarg_0);
                            generator.Emit(OpCodes.Ldc_I4, i);
                            generator.Emit(OpCodes.Callvirt, isDBNullMethod);
                            generator.Emit(OpCodes.Brtrue, endIfLabel);

                            generator.Emit(OpCodes.Ldloc, localV);
                            generator.Emit(OpCodes.Call, getMethod);

                            var tmp = generator.DeclareLocal(p.PropertyType);
                            generator.Emit(OpCodes.Stloc, tmp);


                            generator.Emit(OpCodes.Ldloc, tmp);
                            generator.Emit(OpCodes.Ldarg_0);
                            generator.Emit(OpCodes.Ldc_I4, i);
                            generator.Emit(OpCodes.Call, getValueMethod);

                            generator.Emit(OpCodes.Unbox_Any, dr.GetFieldType(i));
                            if (subPropertyInfo.PropertyType.IsGenericType && subPropertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                            {
                                generator.Emit(OpCodes.Newobj, subPropertyInfo.PropertyType.GetConstructor(subPropertyInfo.PropertyType.GetGenericArguments()));
                            }
                            generator.Emit(OpCodes.Call, setMethod);

                            generator.MarkLabel(endIfLabel);
                            return true;
                        }
                    }
                    else
                    {
                        generator.Emit(OpCodes.Ldloc, localV);
                        generator.Emit(OpCodes.Call, getMethod);
                        var tmp = generator.DeclareLocal(p.PropertyType);
                        generator.Emit(OpCodes.Stloc, tmp);

                        if (this.WorkWithSubClass(generator, tmp, p.PropertyType, fname, dr, i)) return true;
                    }
                }
            }
            return false;
        }

        private static bool CheckIsPrimitiveType(Type type)
        {
            return (type == typeof(object) || Type.GetTypeCode(type) != TypeCode.Object);
        }

        private void OpenConnection()
        {
            switch (this.connection.State)
            {
                case ConnectionState.Broken:
                    this.connection.Close();
                    this.connection.Open();
                    break;
                case ConnectionState.Closed:
                    this.connection.Open();
                    break;
                case ConnectionState.Connecting:
                    break;
                case ConnectionState.Executing:
                    break;
                case ConnectionState.Fetching:
                    break;
                case ConnectionState.Open:
                    break;
            }
        }

        ~DataManager()
        {
            this.Dispose(false);
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (this.command != null)
                {
                    this.command.Dispose();
                }
                if (this.connection != null)
                {
                    this.connection.Close();
                    this.connection.Dispose();
                }
                this.command = null;
                this.connection = null;
            }
        }

    }
}
