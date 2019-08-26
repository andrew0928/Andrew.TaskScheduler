using System;
using System.Collections.Concurrent;
//using System.Collections.Generic;
using System.Threading;

namespace TaskSchedulerWorker
{
    public class Program
    {
        static void Main(string[] args)
        {
            var p = new Program();
            
        }






        private int _workers_count = 5;

        private ConcurrentQueue<TaskInfo> _tasks = new ConcurrentQueue<TaskInfo>();
        private AutoResetEvent[] _waits = null;
        private ManualResetEvent _reset_wait = new ManualResetEvent(false);
        private Thread[] _threads = null;

        public Program()
        {

            //int workers = 5;
            this._threads = new Thread[this._workers_count];
            this._waits = new AutoResetEvent[_workers_count];
            for (int i = 0; i < _workers_count; i++)
            {
                _threads[i] = new Thread(this.ProcessTasksWorker);
                this._waits[i] = new AutoResetEvent(false);

                _threads[i].Start(this._waits[i]);
            }

            Thread update = new Thread(this.UpdateTasksWorker);
            update.Start();
        }


        private void ProcessTasksWorker(object _wait)
        {
            AutoResetEvent wait = (AutoResetEvent)_wait;

            while (true)
            {
                while (true)
                {
                    TaskInfo task;
                    // try get task from queue
                    while (this._tasks.TryDequeue(out task) == false)
                    {
                        //Console.WriteLine("dequeue fail, waiting...");
                        wait.WaitOne();
                        //Console.WriteLine("dequeue fail, waked up...");
                    }

                    // wait until task should run
                    bool skip = false;
                    while (DateTime.Now < task.RunAt)
                    {
                        //if (wait.WaitOne(task.RunAt - DateTime.Now) == true) continue;

                        int result = WaitHandle.WaitAny(
                            new WaitHandle[] { wait, _reset_wait },
                            task.RunAt - DateTime.Now);
                        //Console.WriteLine(result);
                        if (result == 0) continue;
                        if (result == 1) { skip = true; break; }
                    }

                    if (skip == false)
                    {
                        //try
                        //{
                            if (task.Do())
                            {
                                // run
                                Console.WriteLine($"Task: {task.Name}, Delay: {(DateTime.Now - task.RunAt).TotalMilliseconds} msec, RunAt: {task.RunAt}, Current: {DateTime.Now}");
                            }
                            //else
                            //{
                            //    // try run fail
                            //    Console.WriteLine("...");
                            //}
                        //}
                        //catch
                        //{
                        //    Console.WriteLine("___");
                        //}
                    }
                }
            }
        }

        private void UpdateTasksWorker()
        {
            DateTime basetime = DateTime.Now;
            for(int count = 1; count < 10000; count++)
            {
                TaskInfo task = new TaskInfo() { Name = $"Task#{count}", RunAt = basetime.AddMilliseconds(1200 * count) };
                this._tasks.Enqueue(task);
                foreach (var wait in this._waits) wait.Set();
                Console.WriteLine($"Enque Job: {task.Name} (@{task.RunAt})");

                Thread.Sleep(900);

                if (count % 20 == 0)
                {
                    _reset_wait.Set();
                    this._tasks.Clear();
                    _reset_wait.Reset();

                    //foreach (var wait in this._waits) wait.Set();
                    Console.WriteLine($"Clean Queue");
                }
            }


            //while (true)
            //{
            //    Console.WriteLine("Reset Queue...");
            //    this._tasks.Clear();
            //    foreach (var wait in this._waits) wait.Set();

            //    this._tasks = new ConcurrentQueue<TaskInfo>(new TaskInfo[] {
            //        new TaskInfo() { Name = "01", RunAt = DateTime.Now.AddMilliseconds(1500) },
            //        new TaskInfo() { Name = "02", RunAt = DateTime.Now.AddMilliseconds(2000) },
            //        new TaskInfo() { Name = "03", RunAt = DateTime.Now.AddMilliseconds(2500) },
            //        new TaskInfo() { Name = "04", RunAt = DateTime.Now.AddMilliseconds(3000) },
            //        new TaskInfo() { Name = "05", RunAt = DateTime.Now.AddMilliseconds(3500) },
            //        new TaskInfo() { Name = "06", RunAt = DateTime.Now.AddMilliseconds(4000) },
            //        new TaskInfo() { Name = "07", RunAt = DateTime.Now.AddMilliseconds(4500) },
            //        new TaskInfo() { Name = "08", RunAt = DateTime.Now.AddMilliseconds(5000) },
            //        new TaskInfo() { Name = "09", RunAt = DateTime.Now.AddMilliseconds(5500) },
            //        new TaskInfo() { Name = "10", RunAt = DateTime.Now.AddMilliseconds(6000) },
            //    });
            //    foreach (var wait in this._waits) wait.Set();

            //    Thread.Sleep(5000);
            //}
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
            Thread.Sleep(100);

            lock(this._syncroot)
            {
                if (this._state != 1) throw new InvalidOperationException();
                this._state = 2;
            }

            return true;
        }
    }
}
