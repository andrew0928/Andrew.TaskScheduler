using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Dapper;

namespace TaskSchedulerController
{

    public class JobInfo
    {
        public int Id { get; set; }
        public DateTime CreateAt { get; set; }
        public DateTime RunAt { get; set; }
        public DateTime? ExecuteAt { get; set; }

        public int State { get; set; }
    }

    public enum JobStateEnum : int
    {
        CREATE = 0,
        LOCK = 1,
        COMPLETE = 2
    }

    public class JobsRepo : IDisposable
    {
        private SqlConnection _conn = null;
        private readonly string _client = null;

        public JobsRepo(string connstr = null, string client = null)
        {
            if (string.IsNullOrEmpty(client))
            {
                this._client = $"pid:{System.Diagnostics.Process.GetCurrentProcess().Id}";
            }
            else
            {
                this._client = client;
            }

            if (string.IsNullOrEmpty(connstr))
            {
                this._conn = new SqlConnection(@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=JobsDB;Integrated Security=True;Pooling=False");
            }
            else
            {
                this._conn = new SqlConnection(connstr);
            }
        }

        public IEnumerable<JobInfo> GetReadyJobs(TimeSpan duration)
        {
            return this._conn.Query<JobInfo>(
                @"
select * from [jobs] where state = 0 and runat < @until order by runat asc;
insert [workerlogs] (action, clientid, results) values ('QUERYLIST', @client, @@rowcount);
",
                new
                {
                    since = DateTime.Now,
                    until = DateTime.Now + duration,
                    client = this._client,
                });
        }

        public JobInfo GetJob(int jobid)
        {
            return this._conn.QuerySingle<JobInfo>(
                @"
select * from [jobs] where id = @jobid;
insert [workerlogs] (jobid, action, clientid, results) values (@jobid, 'QUERYJOB', @client, 1);
", 
                new {
                    jobid,
                    client = this._client
                });
        }

        public int CreateJob(DateTime runat)
        {
            return _conn.ExecuteScalar<int>(
    @"
--
-- create job definition
--
declare @id int;
insert [jobs] (RunAt) values (@runat); 
set @id = @@identity;
insert [workerlogs] (jobid, action, clientid) values (@id, 'CREATE', @clientid);
select @id;
",
                new
                {
                    runat,
                    clientid = _client,
                });
        }

        public bool AcquireJobLock(int jobId)
        {
            return this._conn.Execute(
                @"
update [jobs] set state = 1 where id = @id and state = 0;
insert [workerlogs] (jobid, action, clientid) values (@id, case @@rowcount when 1 then 'ACQUIRE_SUCCESS' else 'ACQUIRE_FAIL' end, @clientid);
",
                new
                {
                    id = jobId,
                    clientid = this._client,
                }) == 2;
        }

        public bool ProcessLockedJob(int jobId)
        {

            return this._conn.Execute(
                @"
update [jobs] set state = 2, executeat = getdate() where id = @id and state = 1;
insert [workerlogs] (jobid, action, clientid) values (@id, case @@rowcount when 1 then 'COMPLETE' else 'ERROR' end, @clientid);
",
                new
                {
                    id = jobId,
                    clientid = _client,
                }) == 2;
        }

        public void ResetDatabase()
        {
            this._conn.Execute(@"truncate table [jobs]; truncate table [workerlogs];");
        }

        public void Dispose()
        {
            this._conn.Close();
        }
    }




    class Program
    {
        static void Main(string[] args)
        {
            using (var repo = new JobsRepo())
            {
                repo.ResetDatabase();

                int jobid = repo.CreateJob(DateTime.Now.AddSeconds(5));

                var job = repo.GetJob(jobid);

                foreach(var j in repo.GetReadyJobs(TimeSpan.FromDays(1)))
                {
                    Console.WriteLine(j);
                }

                Task.Delay(1000).Wait();

                if (repo.AcquireJobLock(jobid) && repo.ProcessLockedJob(jobid))
                {
                    // success
                    Console.WriteLine($"Job #{jobid} complete.");
                }
            }
        }
    }
}
