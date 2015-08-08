using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace Neyro.Data
{
    /// <summary>
    /// Класс менеджера данных
    /// </summary>
    public class DataManager : IDisposable
    {
        private delegate T DmDelegate<out T>(IDataRecord dr);

        private static readonly Dictionary<Type, Dictionary<int, Delegate>> invokes = new Dictionary<Type, Dictionary<int, Delegate>>();
        private static readonly object invokeLock = new object();
        private static readonly MethodInfo getValueMethod = typeof(IDataRecord).GetMethod("get_Item", new[] { typeof(int) });
        private static readonly MethodInfo isDbNullMethod = typeof(IDataRecord).GetMethod("IsDBNull", new[] { typeof(int) });

        private bool disposed;
        private IDbConnection connection;
        private IDbCommand command;
        private int cmdHashCode;
        
        /// <summary>
        /// Создание экземляра менеджера данных
        /// </summary>
        /// <param name="conn">Соединение с БД</param>
        public DataManager(IDbConnection conn)
        {
            this.connection = conn;
        }

        /// <summary>
        /// Подготавливает выполнение хранимой процедуры
        /// </summary>
        /// <param name="cmdText">Наименование хранимой процедуры</param>
        /// <returns>Ссылка на себя</returns>
        public DataManager Procedure(string cmdText)
        {
            this.command = this.connection.CreateCommand();
            this.command.CommandText = cmdText;
            this.command.CommandType = CommandType.StoredProcedure;
            this.cmdHashCode = cmdText.GetHashCode();
            return this;
        }

        /// <summary>
        /// Подготавливает выполнение Sql запроса
        /// </summary>
        /// <param name="cmdText">Текст Sql запроса</param>
        /// <returns>Ссылка на себя</returns>
        public DataManager Sql(string cmdText)
        {
            this.command = this.connection.CreateCommand();
            this.command.CommandText = cmdText;
            this.command.CommandType = CommandType.Text;
            this.cmdHashCode = cmdText.GetHashCode();
            return this;
        }

        /// <summary>
        /// Добавление параметра к запросу
        /// </summary>
        /// <param name="name">Наименование параметра</param>
        /// <param name="value">Значение параметра</param>
        /// <returns>Ссылка на себя</returns>
        public DataManager AddParam(string name, object value)
        {
            var newPar = this.command.CreateParameter();
            newPar.ParameterName = string.Format("@{0}", name);
            newPar.Value = value ?? DBNull.Value;
            this.command.Parameters.Add(newPar);
            return this;
        }
        /// <summary>
        /// Добавление параметров к запросу
        /// </summary>
        /// <param name="params">Объект, поля которого являются параметрами запроса</param>
        /// <returns>Ссылка на себя</returns>
        public DataManager AddParams(object @params)
        {
            var type = @params.GetType();
            var props = type.GetProperties();

            foreach (var p in props)
            {
                if (p.IsDefined(typeof(NotParamAttribute), true)) continue;
                var newPar = this.command.CreateParameter();
                newPar.ParameterName = string.Format("@{0}", p.Name);
                newPar.Value = p.GetValue(@params, null) ?? DBNull.Value;
                this.command.Parameters.Add(newPar);
            }

            return this;
        }

        /// <summary>
        /// Добавление табличного параметра к запросу
        /// </summary>
        /// <typeparam name="T">Тип табличного параметра</typeparam>
        /// <param name="paramName">Наименование параметра</param>
        /// <param name="paramValue">Значение параметра</param>
        /// <returns>Ссылка на себя</returns>
        public DataManager AddEnumerableParam<T>(string paramName, IEnumerable<T> paramValue)
        {
            var ttype = typeof(T);
            var props = ttype.GetProperties();

            var table = new DataTable();

            table.Columns.AddRange(props.Select(this.Property2DataTableColumns).ToArray());

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

        /// <summary>
        /// Выполнение запроса с получение скалярного результата
        /// </summary>
        /// <typeparam name="T">Тип результата</typeparam>
        /// <returns>Результат</returns>
        public T Scalar<T>()
        {
            this.OpenConnection();
            T result = default(T);
            var r = this.command.ExecuteScalar();
            if (r != null) result = (T)r;
            return result;
        }

        /// <summary>
        /// Выполнение запроса
        /// </summary>
        /// <returns>Кол-во обработанных строк</returns>
        public int Execute()
        {
            this.OpenConnection();
            return this.command.ExecuteNonQuery();
        }

        #region Get
        /// <summary>
        /// Выполнение запроса и получение объекта - результата выборки одиночной строки с возможностью получения любого количества подчиненных записей
        /// </summary>
        /// <typeparam name="T">Тип получаемого объекта</typeparam>
        /// <returns>Результат выборки</returns>
        public T Get<T>()
        {
            this.OpenConnection();
            using (var dr = this.command.ExecuteReader())
            {
                return dr.Read() ? this.Create<T>(dr) : default(T);
            }
        }
        /// <summary>
        /// Выполнение запроса и получение объекта - результата выборки одиночной строки с возможностью получения любого количества подчиненных записей
        /// </summary>
        /// <typeparam name="T">Тип получаемого объекта</typeparam>
        /// <param name="detailCreators">Делегаты для обработки дочерних строк выборки</param>
        /// <returns>Результат выборки</returns>
        public T Get<T>(params Action<IDataRecord, T>[] detailCreators) 
        {
            this.OpenConnection();
            using (var dr = this.command.ExecuteReader())
            {
                var res = dr.Read() ? this.Create<T>(dr) : default(T);
                if (detailCreators != null && detailCreators.Length > 0)
                    foreach (var detailCreator in detailCreators)
                        if (dr.NextResult())
                            while (dr.Read())
                                detailCreator(dr, res);
                return res;
            }
        }

        /// <summary>
        /// Выполнение запроса и получение объекта - результата выборки одиночной строки с возможностью получения результатов подчиненного запроса
        /// </summary>
        /// <typeparam name="T">Тип получаемого объекта</typeparam>
        /// <typeparam name="TA">Тип подчиненного объекта</typeparam>
        /// <param name="subList">Селектор подчиненного объекта</param>
        /// <returns>Результат выборки</returns>
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

        /// <summary>
        /// Выполнение запроса и получение объекта - результата выборки одиночной строки с возможностью получения результатов двух подчиненных запросов
        /// </summary>
        /// <typeparam name="T">Тип получаемого объекта</typeparam>
        /// <typeparam name="TA">Тип подчиненного объекта A</typeparam>
        /// <typeparam name="TB">Тип подчиненного объекта B</typeparam>
        /// <param name="subList">Селектор подчиненного объекта A</param>
        /// <param name="subList2">Селектор подчиненного объекта B</param>
        /// <returns>Результат выборки</returns>
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
        /// <summary>
        /// Выполнение запроса и получение объекта - результата выборки одиночной строки с возможностью получения результатов трех подчиненных запросов
        /// </summary>
        /// <typeparam name="T">Тип получаемого объекта</typeparam>
        /// <typeparam name="TA">Тип подчиненного объекта A</typeparam>
        /// <typeparam name="TB">Тип подчиненного объекта B</typeparam>
        /// <typeparam name="TC">Тип подчиненного объекта С</typeparam>
        /// <param name="subList">Селектор подчиненного объекта A</param>
        /// <param name="subList2">Селектор подчиненного объекта B</param>
        /// <param name="subList3">Селектор подчиненного объекта С</param>
        /// <returns>Результат выборки</returns>
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
        /// <summary>
        /// Выполнение запроса и получение объекта - результата выборки одиночной строки с возможностью получения результатов четырех подчиненных запросов
        /// </summary>
        /// <typeparam name="T">Тип получаемого объекта</typeparam>
        /// <typeparam name="TA">Тип подчиненного объекта A</typeparam>
        /// <typeparam name="TB">Тип подчиненного объекта B</typeparam>
        /// <typeparam name="TC">Тип подчиненного объекта С</typeparam>
        /// <typeparam name="TD">Тип подчиненного объекта D</typeparam>
        /// <param name="subList">Селектор подчиненного объекта A</param>
        /// <param name="subList2">Селектор подчиненного объекта B</param>
        /// <param name="subList3">Селектор подчиненного объекта С</param>
        /// <param name="subList4">Селектор подчиненного объекта D</param>
        /// <returns>Результат выборки</returns>
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
        /// <summary>
        /// Выполнение запроса и получение списка объектов - результата выборки множества строк и неограниченного кол-ва подчиненных выборок
        /// </summary>
        /// <typeparam name="T">Тип получаемых объектов</typeparam>
        /// <returns>Результаты выборки</returns>
        public List<T> GetList<T>()
        {
            this.OpenConnection();
            var res = new List<T>();
            using (var dr = this.command.ExecuteReader())
            {
                var creator = this.GetCreator<T>(dr);
                while (dr.Read()) res.Add(creator(dr));
            }
            return res;
        }
        /// <summary>
        /// Выполнение запроса и получение списка объектов - результата выборки множества строк и неограниченного кол-ва подчиненных выборок
        /// </summary>
        /// <typeparam name="T">Тип получаемых объектов</typeparam>
        /// <param name="detailCreators">Селекторы дочерних выборок</param>
        /// <returns>Результаты выборки</returns>
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

        /// <summary>
        /// Выполнение запроса и получение списка объектов - результата выборки множества строк и одной подчиненной выборки
        /// </summary>
        /// <typeparam name="T">Тип получаемых объектов</typeparam>
        /// <typeparam name="TA">Тип подчиненных объектов выборки A</typeparam>
        /// <param name="sub">Селектор подчиненных объектов A</param>
        /// <returns>Результаты выборки</returns>
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

        /// <summary>
        /// Выполнение запроса и получение списка объектов - результата выборки множества строк и двух подчиненных выборок
        /// </summary>
        /// <typeparam name="T">Тип получаемых объектов</typeparam>
        /// <typeparam name="TA">Тип подчиненных объектов выборки A</typeparam>
        /// <typeparam name="TB">Тип подчиненных объектов выборки B</typeparam>
        /// <param name="subA">Селектор подчиненных объектов A</param>
        /// <param name="subB">Селектор подчиненных объектов A</param>
        /// <returns>Результаты выборки</returns>
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

        /// <summary>
        /// Выполнение запроса и получение списка объектов - результата выборки множества строк и трех подчиненных выборок
        /// </summary>
        /// <typeparam name="T">Тип получаемых объектов</typeparam>
        /// <typeparam name="TA">Тип подчиненных объектов выборки A</typeparam>
        /// <typeparam name="TB">Тип подчиненных объектов выборки B</typeparam>
        /// <typeparam name="TC">Тип подчиненных объектов выборки C</typeparam>
        /// <param name="subA">Селектор подчиненных объектов A</param>
        /// <param name="subB">Селектор подчиненных объектов A</param>
        /// <param name="subC">Селектор подчиненных объектов C</param>
        /// <returns>Результаты выборки</returns>
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

        /// <summary>
        /// Выполнение запроса и получение списка объектов - результата выборки множества строк и четырех подчиненных выборок
        /// </summary>
        /// <typeparam name="T">Тип получаемых объектов</typeparam>
        /// <typeparam name="TA">Тип подчиненных объектов выборки A</typeparam>
        /// <typeparam name="TB">Тип подчиненных объектов выборки B</typeparam>
        /// <typeparam name="TC">Тип подчиненных объектов выборки C</typeparam>
        /// <typeparam name="TD">Тип подчиненных объектов выборки D</typeparam>
        /// <param name="subA">Селектор подчиненных объектов A</param>
        /// <param name="subB">Селектор подчиненных объектов A</param>
        /// <param name="subC">Селектор подчиненных объектов C</param>
        /// <param name="subD">Селектор подчиненных объектов D</param>
        /// <returns>Результаты выборки</returns>
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
        
        /// <summary>
        /// Обработка сырого результата
        /// </summary>
        /// <param name="action">Метод обработки</param>
        public void Raw(Action<IDataReader> action)
        {
            this.OpenConnection();
            using (var dr = this.command.ExecuteReader())
            {
                action(dr);
            }
        }

        /// <summary>
        /// Маппинг объекта из строки выборки
        /// </summary>
        /// <typeparam name="T">Тип объекта</typeparam>
        /// <param name="dr">Текущая запись выборки</param>
        /// <returns>Объект</returns>
        public T Create<T>(IDataRecord dr)
        {
            return this.GetCreator<T>(dr)(dr);
        }

        /// <summary>
        /// Получение столбца таблицы для табличного параметра из свойства
        /// </summary>
        /// <param name="p">Свойство</param>
        /// <returns></returns>
        private DataColumn Property2DataTableColumns(PropertyInfo p)
        {
            return p.PropertyType.IsGenericType && p.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>) ? new DataColumn(p.Name, p.PropertyType.GetGenericArguments()[0]) { AllowDBNull = true } : new DataColumn(p.Name, p.PropertyType);
        }

        /// <summary>
        /// Получение кода создателя объекта из записи выборки
        /// </summary>
        /// <typeparam name="T">Тип объекта</typeparam>
        /// <param name="dr">Запись выборки</param>
        /// <returns>Делегат с кодом создания</returns>
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
                            // ReSharper disable once AssignNullToNotNullAttribute
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
                                        generator.Emit(OpCodes.Callvirt, isDbNullMethod);
                                        generator.Emit(OpCodes.Brtrue, endIfLabel);

                                        generator.Emit(OpCodes.Ldloc, localV);
                                        generator.Emit(OpCodes.Ldarg_0);
                                        generator.Emit(OpCodes.Ldc_I4, i);
                                        generator.Emit(OpCodes.Call, getValueMethod);

                                        generator.Emit(OpCodes.Unbox_Any, dr.GetFieldType(i));
                                        if (propertyInfo.PropertyType.IsGenericType && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                                        {
                                            // ReSharper disable once AssignNullToNotNullAttribute
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
            var props = type.GetProperties().Where(p => name.IndexOf(p.Name, StringComparison.Ordinal) == 0);
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
                            generator.Emit(OpCodes.Callvirt, isDbNullMethod);
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
                                // ReSharper disable once AssignNullToNotNullAttribute
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

        /// <summary>
        /// Проверка, тип относится к примитивным или нет
        /// </summary>
        /// <param name="type">Тип</param>
        /// <returns>Истина, если тип примитивный</returns>
        private static bool CheckIsPrimitiveType(Type type)
        {
            return (type == typeof(object) || Type.GetTypeCode(type) != TypeCode.Object);
        }

        /// <summary>
        /// Открытие соединения с БД
        /// </summary>
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
        /// <summary>
        /// IDisposable.Dispose
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (this.disposed) return;

            if (this.command != null)
            {
                this.command.Dispose();
            }
            if (this.connection != null)
            {
                this.connection.Close();
                this.connection.Dispose();
            }

            if (disposing)
            {
                this.command = null;
                this.connection = null;
            }

            this.disposed = true;
        }

    }
}
