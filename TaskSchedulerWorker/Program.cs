using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Collections.Generic;

namespace TaskSchedulerWorker
{
    public class Program
    {
        static void Main(string[] args)
        {
            new HostBuilder()
                .ConfigureServices((context, services) =>
                {
                    //services.AddSingleton<TaskInfoCollection>();

                    //services.AddHostedService<DispatchWorker>();
                    //services.AddHostedService<FetchWorker>();

                    services.AddHostedService<ScheduleWorker>();
                })
                .RunConsoleAsync()
                .Wait();
        }

    }


    public class ScheduleWorker : BackgroundService
    {
        private TaskInfoCollection _tasks = null;
        private CancellationToken _stop;

        private readonly int _workers = 5;

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            this._tasks = new TaskInfoCollection();
            this._stop = stoppingToken;

            Thread fetch = new Thread(this.FetchThread);
            fetch.Start();

            Thread[] dispatches = new Thread[this._workers];
            for (int i=0; i < this._workers; i++)
            {
                Thread dispatch = new Thread(this.DispatchThread);
                dispatch.Start();
                dispatches[i] = dispatch;
            }

            await stoppingToken.WaitHandle.WaitOneAsync();

            fetch.Join();
            foreach (var d in dispatches) d.Join();
        }


        private void FetchThread()
        {
            DateTime basetime = DateTime.Now;

            for (int count = 1; count < 10000; count++)
            {
                if (this._stop.IsCancellationRequested) break;

                TaskInfo task = new TaskInfo()
                {
                    Name = $"Task#{count}",
                    RunAt = basetime.AddMilliseconds(1200 * count)
                };

                this._tasks.Set(task);
                Console.WriteLine($"Enque Job: {task.Name} (@{task.RunAt})");

                Task.Delay(900).Wait();

                if (count % 10 == 0)
                {
                    this._tasks.Clear();
                    Console.WriteLine($"Clean Queue");
                }
            }

            this._tasks.Complete();
        }

        private void DispatchThread()
        {
            while (this._stop.IsCancellationRequested == false)
            {
                TaskInfo task = this._tasks.GetAsync().Result;

                if (task == null) continue;


                if ((DateTime.Now < task.RunAt) && this._tasks.WaitClearAsync(task.RunAt - DateTime.Now).Result)
                {
                    // reset, abort current task, reget & redo
                    continue;
                }

                if (task.Do())
                {
                    // get lock, run
                    Console.WriteLine($"Task: {task.Name}, Delay: {(DateTime.Now - task.RunAt).TotalMilliseconds} msec, RunAt: {task.RunAt}, Current: {DateTime.Now}");
                }
            }
        }
    }



    public class FetchWorker : BackgroundService
    {
        private TaskInfoCollection _tasks = null;

        public FetchWorker(TaskInfoCollection tasks)
        {
            this._tasks = tasks;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            DateTime basetime = DateTime.Now;

            for (int count = 1; count < 10000; count++)
            {
                if (stoppingToken.IsCancellationRequested) break;

                TaskInfo task = new TaskInfo()
                {
                    Name = $"Task#{count}",
                    RunAt = basetime.AddMilliseconds(1200 * count)
                };

                this._tasks.Set(task);
                Console.WriteLine($"Enque Job: {task.Name} (@{task.RunAt})");

                await Task.Delay(900);

                if (count % 10 == 0)
                {
                    this._tasks.Clear();
                    Console.WriteLine($"Clean Queue");
                }
            }

            this._tasks.Complete();
        }

    }

    public class DispatchWorker : BackgroundService
    {
        private TaskInfoCollection _tasks = null;
        public DispatchWorker(TaskInfoCollection tasks)
        {
            this._tasks = tasks;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (stoppingToken.IsCancellationRequested == false)
            {
                TaskInfo task = await this._tasks.GetAsync();

                if (task == null) continue;


                if ((DateTime.Now < task.RunAt) && await this._tasks.WaitClearAsync(task.RunAt - DateTime.Now))
                {
                    // reset, abort current task, reget & redo
                    continue;
                }

                if (task.Do())
                {
                    // get lock, run
                    Console.WriteLine($"Task: {task.Name}, Delay: {(DateTime.Now - task.RunAt).TotalMilliseconds} msec, RunAt: {task.RunAt}, Current: {DateTime.Now}");
                }
            }
        }
    }


    public class TaskInfoCollection
    {
        private ConcurrentQueue<TaskInfo> _tasks = new ConcurrentQueue<TaskInfo>();
        private AutoResetEvent _wait = new AutoResetEvent(false);
        private ManualResetEvent _reset = new ManualResetEvent(false);
        private WaitHandle[] _all_waits = null;

        public TaskInfoCollection()
        {
            this._all_waits = new WaitHandle[]
            {
                this._wait,
                this._reset
            };
        }

        public async Task<TaskInfo> GetAsync()
        {
            TaskInfo task;
            while(this._tasks.TryDequeue(out task) == false)
            {
                if (await this.WaitNotifyAsync() == 1) return null;
            }
            return task;
        }

        private object _syncroot = new object();

        public void Set(TaskInfo task)
        {
            lock (this._syncroot) this._tasks.Enqueue(task);
            this._wait.Set();
        }

        public void Clear()
        {
            this._reset.Set();
            lock (this._syncroot) this._tasks.Clear(); /* add items in atom operation */
            this._reset.Reset();
        }

        public void Complete()
        {
            this._reset.Set();
            lock (this._syncroot) this._tasks.Clear();
        }


        // return 0: enqueue
        // return 1: clean
        private async Task<int> WaitNotifyAsync()
        {
            return await Task.Run<int>(() => { return WaitHandle.WaitAny(this._all_waits); });
        }

        public async Task<bool> WaitClearAsync(TimeSpan duration)
        {
            return await Task.Run<bool>(() => { return this._reset.WaitOne(duration); });
        }
    }


    public class TaskInfo
    {
        public DateTime RunAt = DateTime.MaxValue;

        public string Name;

        private object _syncroot = new object();

        private int _state = 0; // 0: ready, 1: running, 2: done

        public bool Do()
        {
            if (this._state > 0) return false;

            lock (this._syncroot)
            {
                if (this._state != 0) return false;
                this._state = 1;
            }

            // do...

            lock (this._syncroot)
            {
                if (this._state != 1) throw new InvalidOperationException();
                this._state = 2;
            }

            return true;
        }
    }

    public static class WaitHandleExtensions
    {
        public static async Task<bool> WaitOneAsync(this WaitHandle context)
        {
            return await Task.Run<bool>(() => { return context.WaitOne(); });
        }
    }
}
