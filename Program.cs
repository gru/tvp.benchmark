using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Bogus;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using DataType = LinqToDB.DataType;

namespace TVP.Benchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            BenchmarkRunner
                .Run<SqlServerTVP>();
        }
    }
    
    [SimpleJob(RuntimeMoniker.Net50)]
    public class SqlServerTVP
    {
        private TestDataConnection _db;
        private Buffer<DataParameter> _buffer;
        private Buffer<string> _stringBuffer;

        [Params(1000)]
        public int TestDataRowCount { get; set; }
        
        [Params(3)]
        public int TestParameterRowCount { get; set; }
        
        [Params(100)]
        public int TestParameterCount { get; set; }
        
        [GlobalSetup]
        public void Setup()
        {
            _db = new TestDataConnection(
                ProviderName.SqlServer2017,
                "Server=localhost;Database=master;User Id=sa;password=smetana2018_;");

            Console.WriteLine("Prepare database...");

            _db.Execute(@"
USE master;

IF EXISTS (SELECT * FROM sys.databases WHERE name = 'MyTestDataBase')
    BEGIN
        DROP DATABASE MyTestDataBase;
    END;

CREATE DATABASE MyTestDataBase;
ALTER DATABASE MyTestDataBase ADD FILEGROUP MyTestDataBase_MOD
    CONTAINS MEMORY_OPTIMIZED_DATA;
ALTER DATABASE MyTestDataBase ADD FILE (
    name='MyTestDataBase_MOD1', filename='/var/opt/mssql/data/MyTestDataBase_MOD1')
    TO FILEGROUP MyTestDataBase_MOD;");
            
            _db.Execute(@"
USE MyTestDataBase;

CREATE TYPE SimpleUDT AS TABLE
(Id BIGINT);

CREATE TYPE SimplePKUDT AS TABLE
(Id BIGINT PRIMARY KEY NONCLUSTERED);

CREATE TYPE MemoryOptUDT AS TABLE
(Id BIGINT PRIMARY KEY NONCLUSTERED);

CREATE TYPE MemoryOptHashUDT AS TABLE
(Id BIGINT PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT = 32))
    WITH ( MEMORY_OPTIMIZED = ON );

CREATE TABLE TestData
(Id BIGINT PRIMARY KEY IDENTITY, 
 Name nvarchar(256));
");
            
            Console.WriteLine("Prepare database completed");
            Console.WriteLine("Prepare data...");
            
            var faker = new Faker<TestData>()
                .RuleFor(d => d.Name, f => $"{f.Name.FirstName()} {f.Name.LastName()}");

            var data = faker.Generate(TestDataRowCount);
            foreach (var row in data)
                _db.Insert(row);

            var random = new Random();
            var parameters = new List<DataParameter>(TestParameterCount);
            var strings = new List<string>();
            var set = new HashSet<long>(TestParameterRowCount);
            for (var parameterIndex = 0; parameterIndex < TestParameterCount; parameterIndex++)
            {
                var table = new DataTable();
                table.Columns.Add("Id");

                do
                {
                    set.Add(random.Next(0, TestDataRowCount + 1));
                } while (set.Count < TestParameterRowCount);

                foreach (var value in set)
                    table.Rows.Add(value);

                strings.Add(string.Join(";", set));
                
                set.Clear();
                
                parameters.Add(new DataParameter("ids", table, DataType.Structured));
            }
            
            _buffer = new Buffer<DataParameter>(parameters);
            _stringBuffer = new Buffer<String>(strings);
            
            Console.WriteLine("Prepare data completed");
        }

        [GlobalCleanup]
        public void CleanUp()
        {
            _db.Dispose();
        }
        
        [Benchmark(OperationsPerInvoke = 1000)]
        public void StringSplit()
        {
            for (var i = 0; i < 1000; i++)
            {
                var p = _stringBuffer.Peek();
                
                var _ = _db.GetTable<TestData>()
                    .Where(t => _db.SplitString(p, ";")
                        .Select(v => v.Value)
                        .Select(ri => Sql.Convert<long, string>(ri))
                        .Contains(t.Id))
                    .ToArray();
            }
        }
        
        [Benchmark(OperationsPerInvoke = 1000)]
        public void SimpleUDT()
        {
            for (var i = 0; i < 1000; i++)
            {
                var p = _buffer.Peek();
                p.DbType = "[dbo].[SimpleUDT]";
                var query = from r in _db.FromSql<UDTRow>($"{p}") select r.Id;
        
                var _ = _db.GetTable<TestData>()
                    .Where(t => query.Contains(t.Id))
                    .ToArray();
            }
        }
        
        [Benchmark(OperationsPerInvoke = 1000)]
        public void SimplePKUDT()
        {
            for (var i = 0; i < 1000; i++)
            {
                var p = _buffer.Peek();
                p.DbType = "[dbo].[SimplePKUDT]";
                var query = from r in _db.FromSql<UDTRow>($"{p}") select r.Id;
        
                var _ = _db.GetTable<TestData>()
                    .Where(t => query.Contains(t.Id))
                    .ToArray();
            }
        }
        
        [Benchmark(OperationsPerInvoke = 1000)]
        public void MemoryOptUDT()
        {
            for (var i = 0; i < 1000; i++)
            {
                var p = _buffer.Peek();
                p.DbType = "[dbo].[MemoryOptUDT]";
                var query = from r in _db.FromSql<UDTRow>($"{p}") select r.Id;
        
                var _ = _db.GetTable<TestData>()
                    .Where(t => query.Contains(t.Id))
                    .ToArray();
            }
        }
        
        [Benchmark(OperationsPerInvoke = 1000)]
        public void MemoryOptHashUDT()
        {
            for (var i = 0; i < 1000; i++)
            {
                var p = _buffer.Peek();
                p.DbType = "[dbo].[MemoryOptHashUDT]";
                var query = from r in _db.FromSql<UDTRow>($"{p}") select r.Id;
        
                var _ = _db.GetTable<TestData>()
                    .Where(t => query.Contains(t.Id))
                    .ToArray();
            }
        }
    }

    [Table(Name = "TestData")]
    public sealed class TestData
    {
        [Column("Id"), PrimaryKey, Identity]
        public long Id { get; set; }
        
        [Column("Name"), MaxLength(256)]
        public string Name { get; set; }
    }

    internal record UDTRow
    {
        public long Id { get; set; }
    }

    public sealed class SplitStringRow
    {
        public string Value { get; set; }
    }
    
    internal sealed class Buffer<T>
    {
        private readonly List<T> _data;
        private int _cursor;
        
        public Buffer(List<T> data)
        {
            _data = data;
        }

        public T Peek()
        {
            var result = _data[_cursor++];
            if (_cursor > _data.Count - 1)
                _cursor = 0;
            return result;
        }
    }

    public class TestDataConnection : DataConnection
    {
        public TestDataConnection(String provider, String cs)
            : base(provider, cs)
        {
        }

        [Sql.TableFunction(Name = "string_split")]
        public ITable<SplitStringRow> SplitString(string p1, string p2)
        {
            var methodInfo = GetType().GetMethod("SplitString", BindingFlags.Public | BindingFlags.Instance);
            return GetTable<SplitStringRow>(this, methodInfo, p1, p2);
        }
    }
}