
using System;
using System.Threading.Tasks;
using System.Net;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using System.Threading;
using Newtonsoft.Json;
using System.Linq;

namespace PiAware2SQLV4
{
    /* 
     * 05/25/2020 Mark Moore
     * This program will read json from a Raspberry Pi running PiAware software, parse it and write the values out to two different SQL databases.  One Azure SQL Database, one on premises.
     * 
     * 12/11/2020 Mark Moore
     * Rewrote the program in C# from Python
     * 
     * 01/17/2021 Mark Moore
     * Updated program to use the new dump 1090 column format which is installed with PiAware 4.0
     */

    static class Program
    {
        private static SqlConnection con1;
        private static SqlConnection con2;
        /// <summary>
        public class Flightrec
        {
            public DateTime dt { get; set; }            // Date Time from system
            public string hex { get; set; }             // Transponders unique hex code, 24-bit ICAO code
            public string flight { get; set; }          // Flight Number as filed
            public double alt_baro { get; set; }        // Barometric altitude of the aircraft
            public double alt_geom { get; set; }        // Geometric altitude of the aircraft
            public double gs { get; set; }              // Ground speed in knots
            public double track { get; set; }           // True track angle in degrees
            public double baro_rate { get; set; }       // Barometric rate of change of altitude in feet/minute
            public string squawk { get; set; }          // Aircraft assigned squawk code
            public string emergency { get; set; }       // Whether or not a captain or crew has indicated plane is in a state of emergency
            public string category { get; set; }        // indicates what type of transmission equipment is on board: Class A1, A1S, A2, A3, B1S or B1 equipment
            public double nav_qnh { get; set; }         //Related to QNH, which is a barometer corrected for ground altitude
            public double nav_altitude_mcp { get; set; }//?
            public double nav_heading { get; set; }     //?
            public string nav_modes { get; set; }       // Navigation mode such as autopilot
            public double lat { get; set; }             // Latitude of current position
            public double lon { get; set; }             // Longitude of current position
            public double nic { get; set; }             // Navigation Integrity Category (containment radius around the aircrafts reported position.  Ranges from 0 to 11 where 0 is unknown and 11 is <7.5 miles)
            public double rc { get; set; }              // Navigation Accuracy Category for Position
            public double seen_pos { get; set; }        // Navigation Accuracy Category for Velocity
            public int version { get; set; }            // DO-260, DO-260A, or DO-260B, version 0, 1 or 2
            public string sil { get; set; }             // Source Integrity Level (SIL) indicates the probability of the reported horizontal position exceeding the containment radius defined by the NIC
            public int gva { get; set; }                // Geometric Vertical Accuracy (GVA);  Accuracy of vertical geometric position 0>150meters 1<=150meters 2<45meters
            public string sda { get; set; }             // System Designed Assurance indicates the probability of an aircraft malfunction causing false or misleading info to be transmitted
            public string mlat { get; set; }            // An object array that defines what values have been derived from MLAT rather than the antenna
            public string tisb { get; set; }            // Traffic information Service Broadcast
            public double messages { get; set; }        // Total number of mode 5 messages received from the aircraft
            public double seen { get; set; }            // How long ago before now in seconds a message was last received from the aircraft
            public double rssi { get; set; }            // Recent average signal power in dbFS
            public string acode { get; set; }           // Airline Code
            public double distance { get; set; }        // Distance in miles calucalted using the Haversign formula
        }

        public static double toRadians(double angle)
        /*
         * Function to convert degrees to radians
         */
        {
            return Math.PI * angle / 180.0;
        }

        private static double Miles(double lat1, double lat2, double lon1, double lon2)
        /* Funciton to calculate distance in miles from two lat/lon coordinaces
         * I got this from the Haversine formula:  https://en.wikipedia.org/wiki/Haversine_formula
         */
        {
            var R = 6372.8; // In kilometers
            var dLat = toRadians(lat2 - lat1);
            var dLon = toRadians(lon2 - lon1);
            lat1 = toRadians(lat1);
            lat2 = toRadians(lat2);

            var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) + Math.Sin(dLon / 2) * Math.Sin(dLon / 2) * Math.Cos(lat1) * Math.Cos(lat2);
            var c = 2 * Math.Asin(Math.Sqrt(a));
            var miles = R * 2 * Math.Asin(Math.Sqrt(a));
            return miles * 0.62137119;  // Convert KM to Miles.
        }

        static SqlConnection AzureSqlconnect()
        /*
         * Set up a connection to Database 1
         */
        {
            var localcon = new SqlConnection();
            localcon.ConnectionString = "Data Source=piawaredbserver.database.windows.net;Initial Catalog=PiAwaredb;user id=PiAware;Password=Demopass@word1";
            localcon.Open();

            return localcon;
        }

        static SqlConnection SQLDBconnect()
        /*
         * Set up a connection to Database 2
         */
        {
            var localcon2 = new SqlConnection();
            localcon2.ConnectionString = "Data Source=SQLDB;Initial Catalog=PiAwaredb;Integrated Security = True";
            localcon2.Open();

            return localcon2;
        }

        public static class StaticItems
        /*
         * Set up endpoint to Dump 1090 on Raspberry Pi to pull Json
         */
        {
            public static string EndPoint = "http://192.168.0.129/dump1090-fa/data/aircraft.json";


        }
        static void Main()
        {
            con1 = AzureSqlconnect();
            con2 = SQLDBconnect();
            double mylat = 33.076153;  //**  My   **
            double mylon = -97.10859;  //** House **

            /*
             **** Microsoft Las Colinas Office ****
             ****      GPS Coordinates         ****
            //32.900076025507246, -96.96343451541534
            */

            /* 
             ****  Variables used in main processing loop  ****
            */
            double distance = 0;
            var tcount = 0;

            var webClient = new WebClient();
            webClient.BaseAddress = StaticItems.EndPoint;

            Console.Clear();

            while (true)
            {
                try
                {
                    var json = webClient.DownloadString("aircraft.json");

                    JToken token = JToken.Parse(json);
                    JArray aircraft = (JArray)token.SelectToken("aircraft");
                    JArray saircraft = new JArray(aircraft.OrderBy(obj => (string)obj["flight"]));
                    //Console.Clear();
                    Console.SetCursorPosition(0, 0);
                    Console.WriteLine("----------------------------------------------------------------------------------------");
                    Console.WriteLine("|                        --  Write to two SQL Databases                                |");
                    Console.WriteLine("----------------------------------------------------------------------------------------");
                    Console.WriteLine("Flight     |    Lat     |    Lon     |  Altitude  |   Speed    |  Vertical  | Emergency|");
                    Console.WriteLine("-----------+------------+------------+------------+------------+------------+-----------");

                    var i = 0;

                    foreach (JToken ac in saircraft)
                    {
                        if (ac["hex"] != null &
                            ac["flight"] != null &
                            ac["lat"] != null &
                            ac["lon"] != null &
                            ac["alt_baro"] != null &
                            ac["baro_rate"] != null &
                            ac["track"] != null &
                            ac["gs"] != null)

                        {
                            i++;

                            Flightrec prec = new Flightrec
                            {
                                dt = DateTime.Now,
                                hex = Convert.ToString(ac["hex"]),
                                flight = Convert.ToString(ac["flight"]),
                                alt_baro = Convert.ToDouble(ac["alt_baro"]),
                                alt_geom = Convert.ToDouble(ac["alt_geom"]),
                                gs = Convert.ToDouble(ac["gs"]),
                                track = Convert.ToDouble(ac["mach"]),
                                baro_rate = Convert.ToDouble(ac["baro_rate"]),
                                squawk = Convert.ToString(ac["squawk"]),
                                emergency = Convert.ToString(ac["emergency"]),
                                category = Convert.ToString(ac["category"]),
                                nav_altitude_mcp = Convert.ToDouble(ac["nav_altitude_mcp"]),
                                nav_qnh = Convert.ToDouble(ac["nav_qnh"]),
                                nav_heading = Convert.ToDouble(ac["nav_heading"]),
                                nav_modes = Convert.ToString(ac["nav_modes"]),
                                lat = Convert.ToDouble(ac["lat"]),
                                lon = Convert.ToDouble(ac["lon"]),
                                nic = Convert.ToDouble(ac["nic"]),
                                rc = Convert.ToDouble(ac["rc"]),
                                seen_pos = Convert.ToDouble(ac["seen_pos"]),
                                version = Convert.ToInt32(ac["version"]),
                                sil = Convert.ToString(ac["sil"]),
                                gva = Convert.ToInt32(ac["gva"]),
                                sda = Convert.ToString(ac["sda"]),
                                mlat = Convert.ToString(ac["mlat"]),
                                tisb = Convert.ToString(ac["tisb"]),
                                messages = Convert.ToDouble(ac["messages"]),
                                seen = Convert.ToDouble(ac["seen"]),
                                rssi = Convert.ToDouble(ac["rssi"]),
                                acode = Convert.ToString(ac["flight"]).Substring(0, 3),  // pull the first three characters from flight as the Airline Code,
                                distance = distance = Miles(Convert.ToDouble(ac["lat"]), mylat, Convert.ToDouble(ac["lon"]), mylon)
                            };

                            string dflight = prec.flight.PadRight(10);
                            string dlat = Convert.ToString(prec.lat).PadLeft(10);
                            string dlon = Convert.ToString(prec.lon).PadLeft(10);
                            string dalt = Convert.ToString(prec.alt_geom).PadLeft(10);
                            string dgs = Convert.ToString(prec.gs).PadLeft(10);
                            string dvr = Convert.ToString(prec.baro_rate).PadLeft(10);
                            string dem = Convert.ToString(prec.emergency).PadLeft(7);

                            SqlCommand cmd1 = new SqlCommand();
                            cmd1.Connection = con1;

                            SqlCommand cmd2 = new SqlCommand();
                            cmd2.Connection = con2;

                            try
                            {
                                con1.Open();
                            }
                            catch { }
                          
                            try
                            {
                                con2.Open();
                            }
                            catch { }

                            cmd1.CommandText = "INSERT INTO KDFW (dt,hex,squawk,flight,lat,lon,distance, nucp,seen_pos,altitude,vr,track,speed,category,messages,seen,rssi,acode)   VALUES(@param1,@param2,@param3,@param4,@param5,@param6,@param7,@param8,@param9,@param10,@param11,@param12,@param13,@param14,@param15,@param16,@param17,@param18)";

                            cmd1.Parameters.AddWithValue("@param1", prec.dt);
                            cmd1.Parameters.AddWithValue("@param2", prec.hex);
                            cmd1.Parameters.AddWithValue("@param3", prec.squawk);
                            cmd1.Parameters.AddWithValue("@param4", prec.flight);
                            cmd1.Parameters.AddWithValue("@param5", prec.lat);
                            cmd1.Parameters.AddWithValue("@param6", prec.lon);
                            cmd1.Parameters.AddWithValue("@param7", distance);
                            cmd1.Parameters.AddWithValue("@param8", prec.emergency);
                            cmd1.Parameters.AddWithValue("@param9", prec.seen_pos);
                            cmd1.Parameters.AddWithValue("@param10", prec.alt_baro);
                            cmd1.Parameters.AddWithValue("@param11", prec.baro_rate);
                            cmd1.Parameters.AddWithValue("@param12", prec.track);
                            cmd1.Parameters.AddWithValue("@param13", prec.gs);
                            cmd1.Parameters.AddWithValue("@param14", prec.category);
                            cmd1.Parameters.AddWithValue("@param15", prec.messages);
                            cmd1.Parameters.AddWithValue("@param16", prec.seen);
                            cmd1.Parameters.AddWithValue("@param17", prec.rssi);
                            cmd1.Parameters.AddWithValue("@param18", prec.acode);

                            try
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(DateTime.Now + ": " + "Connection timeout retrying..." + ex);
                                con1.Close();
                                con1.Open();
                                cmd1.ExecuteNonQuery();
                            }

                            cmd2.CommandText = "INSERT INTO KDFW (dt,hex,squawk,flight,lat,lon,distance, nucp,seen_pos,altitude,vr,track,speed,category,messages,seen,rssi,acode)   VALUES(@param1,@param2,@param3,@param4,@param5,@param6,@param7,@param8,@param9,@param10,@param11,@param12,@param13,@param14,@param15,@param16,@param17,@param18)";

                            cmd2.Parameters.AddWithValue("@param1", prec.dt);
                            cmd2.Parameters.AddWithValue("@param2", prec.hex);
                            cmd2.Parameters.AddWithValue("@param3", prec.squawk);
                            cmd2.Parameters.AddWithValue("@param4", prec.flight);
                            cmd2.Parameters.AddWithValue("@param5", prec.lat);
                            cmd2.Parameters.AddWithValue("@param6", prec.lon);
                            cmd2.Parameters.AddWithValue("@param7", distance);
                            cmd2.Parameters.AddWithValue("@param8", prec.emergency);
                            cmd2.Parameters.AddWithValue("@param9", prec.seen_pos);
                            cmd2.Parameters.AddWithValue("@param10", prec.alt_baro);
                            cmd2.Parameters.AddWithValue("@param11", prec.baro_rate);
                            cmd2.Parameters.AddWithValue("@param12", prec.track);
                            cmd2.Parameters.AddWithValue("@param13", prec.gs);
                            cmd2.Parameters.AddWithValue("@param14", prec.category);
                            cmd2.Parameters.AddWithValue("@param15", prec.messages);
                            cmd2.Parameters.AddWithValue("@param16", prec.seen);
                            cmd2.Parameters.AddWithValue("@param17", prec.rssi);
                            cmd2.Parameters.AddWithValue("@param18", prec.acode);

                            try
                            {
                                cmd2.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(DateTime.Now + ": " + "Connection timeout retrying..." + ex);
                                con2.Close();
                                con2.Open();
                                cmd2.ExecuteNonQuery();
                            }

                            Console.WriteLine(dflight + " | " + dlat + " | " + dlon + " | " + dalt + " | " + dgs + " | " + dvr + " | " + dem + "  | " + "                ");

                        }

                    }

                    tcount = i;

                }
                catch { }
                //Console.WriteLine(prec);

                Console.WriteLine("----------------------------------------------------------------------------------------");
                Console.WriteLine("                                                                                        ");
                Console.WriteLine("                                                                                        ");
                Thread.Sleep(1000);
            }
        }
    }
}