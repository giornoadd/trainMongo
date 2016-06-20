namespace mongograph
{
    using MongoDB.Bson;
    using MongoDB.Bson.Serialization.Attributes;
    using MongoDB.Driver;
    using MongoDB.Driver.Linq;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public interface IEntity
    {
        ObjectId Id { get; set; }
    }

    public class passenger : IEntity
    {
        public ObjectId Id { get; set; }

        public string Name { get; set; }

        [BsonElement("trainCarId")]
        public ObjectId trainCarId { get; set; }

        [BsonIgnore]
        public trainCar trainCar { get; set; }
    }


    public class trainCar : IEntity
    {
        public ObjectId Id { get; set; }

        public string carNo { get; set; }

        public string serial { get; set; }

        public string note { get; set; }

        [BsonElement("trainId")]
        public ObjectId trainId { get; set; }

        [BsonIgnore]
        public IList<passenger> passengers { get; set; }

        [BsonIgnore]
        public train train { get; set; }
    }

    public class train : IEntity
    {
        public ObjectId Id { get; set; }

        public string trainNo { get; set; }

        public string note { get; set; }

        [BsonIgnore]
        public IList<trainCar> trainCars { get; set; }
    }

    internal class Program
    {
        private static void Main(string[] args)
        {
            var server = new MongoClient("mongodb://localhost");
            var database = server.GetDatabase("testmongodb");
            foreach (var collectionName in database.ListCollections().ToList())
                database.DropCollection(collectionName[0].AsString);

            var collection = database.GetCollection<trainCar>("trainCars");
            var trainCollection = database.GetCollection<train>("trains");
            var peopleCollection = database.GetCollection<passenger>("passengers");


            //add some data
            for (int t = 0; t < 20; t++)
            {
                train newTrain = new train();
                newTrain.note = "nyc" + t.ToString();
                newTrain.trainNo = "345";
                newTrain.trainCars = new List<trainCar>();
                trainCollection.InsertOne(newTrain);


                //add some cars:

                for (int i = 0; i < 50; i++)
                {
                    trainCar tcar = new trainCar();

                    tcar.carNo = "0" + i.ToString();
                    tcar.note = "Needs new brakes";
                    tcar.serial = "1234";
                    tcar.trainId = newTrain.Id;
                    tcar.passengers = new List<passenger>();
                    collection.InsertOne(tcar);

                    newTrain.trainCars.Add(tcar);

                    for (int p = 0; p < 3; p++)
                    {
                        passenger pass = new passenger();
                        pass.trainCarId = tcar.Id;
                        pass.Name = "name" + p.ToString();
                        peopleCollection.InsertOne(pass);
                        tcar.passengers.Add(pass);
                    }

                }




            }
            Console.WriteLine("saved");


            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            //get the data
            //asparallel gained me about 15% of qry time
            var query = from v in trainCollection.AsQueryable<train>().AsParallel()
                        select v;

            foreach (train aTrain in query)
            {
                GetTraincars(aTrain, database);
                Console.WriteLine("train: " + aTrain.note);
                foreach (trainCar tcar2 in aTrain.trainCars)
                {
                    if (tcar2 != null)
                    {
                        Console.WriteLine("car: " + tcar2.carNo);
                        GetPassengers(tcar2, database);
                        foreach (passenger pass in tcar2.passengers)
                        {
                            Console.WriteLine("car: " + tcar2.carNo + " pass: " + pass.Name);
                        }
                    }
                }


            }
            Console.WriteLine("trains loaded");

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;

            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);
            Console.WriteLine("RunTime " + elapsedTime);

            Console.ReadLine();
        }

        public static void GetPassengers(trainCar tc, IMongoDatabase db)
        {
            var passengerCol = db.GetCollection<passenger>("passengers");
            var trainCarCol = db.GetCollection<passenger>("trainCars");
            var pp = from tcar in trainCarCol.AsQueryable()
                     where tcar.Id == tc.Id
                     join p in passengerCol on tcar.Id equals p.trainCarId into joined
                     from sub_o in joined.DefaultIfEmpty()
                     select sub_o;

            tc.passengers = pp.ToList<passenger>();
        }

        public static void GetTraincars(train t, IMongoDatabase db)
        {
            var tcars = db.GetCollection<trainCar>("trainCars");
            var trains = db.GetCollection<train>("trains");
            var pp = from train in trains.AsQueryable()
                     where train.Id == t.Id
                     join tc in tcars on train.Id equals tc.trainId into joined
                     from sub_o in joined.DefaultIfEmpty()
                     select sub_o;

            t.trainCars = pp.ToList<trainCar>();

        }
    }
}
