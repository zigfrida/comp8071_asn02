using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.IO;

namespace sqltest
{
    class Program
    {
        static readonly string textFile = @"/Users/amandagolubics/Desktop/Test02/text.txt";

        private static void createTables(SqlConnection connection) {
            string createCustomerTable = "create table customer (ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(), Name char(20), Street char(20), City char(20), Photo     VARBINARY(MAX), Gender char(20), birthdate DATE);";
            string createEmployeeTable = "create table employee (ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(), Name char(20), Street char(20), City char(20), Photo VARBINARY(MAX), ManagerID int, JobTitle char(20), Certification char(100), Salary int);";
            string createServiceTypeTable = "create table ServiceType (ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(), Name char(20), CertificationRqts char(100), Rate int);";
            string createCustomerServiceScheduleTable = "create table CustomerServiceSchedule(CustomerID UNIQUEIDENTIFIER, ServiceTypeID UNIQUEIDENTIFIER, EmployeeID UNIQUEIDENTIFIER, StartDateTime date, ExpectedDuration int, ActualDuration int, Status char(20), PRIMARY KEY(CustomerID, ServiceTypeID, EmployeeID, StartDateTime), FOREIGN KEY (CustomerID) REFERENCES Customer(ID), FOREIGN KEY (ServiceTypeID) REFERENCES ServiceType(ID), FOREIGN KEY (EmployeeID) REFERENCES Employee(id));";

            string createShiftDimTable = "create table shiftdim (ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(), Name char(20),  WeekDay CHAR(20), MonthDay int, Month int, Quarter int, Year int);";
            string createCustomerDimTable = "create table customerdim (ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, Name char(20), Street char(20), City char(20), Gender char(20), birthdate DATE, Age int);";
            string createEmployeeDimTable = "create table employeedim (ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, Name char(20), Street char(20), City char(20), ManagerID int, JobTitle char(20), Certification char(100), Salary int);";
            string createServiceTypeDimTable = "create table servicetypedim (ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, Name char(20), CertificationRqts char(100), Rate int);";
            string createCustomerServiceScheduleFactsTable = "create table customerserviceschedulefacts (ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(), CustomerID UNIQUEIDENTIFIER, ServiceTypeID UNIQUEIDENTIFIER, EmployeeID UNIQUEIDENTIFIER, ShiftID UNIQUEIDENTIFIER, StartDateTime date, ExpectedDurationTimeUnits int, ActualDurationTimeUnits int, Status char(20), FOREIGN KEY(CustomerID) REFERENCES customerdim(ID),FOREIGN KEY(ServiceTypeID) REFERENCES servicetypedim(ID), FOREIGN KEY(EmployeeID) REFERENCES employeedim(ID), FOREIGN KEY(ShiftID) REFERENCES shiftdim(ID));";

            
            SqlCommand custdb = new SqlCommand(createCustomerTable, connection);
            custdb.ExecuteNonQuery();
            SqlCommand empdb = new SqlCommand(createEmployeeTable, connection);
            empdb.ExecuteNonQuery();
            SqlCommand servtydb = new SqlCommand(createServiceTypeTable, connection);
            servtydb.ExecuteNonQuery();
            SqlCommand custservdb = new SqlCommand(createCustomerServiceScheduleTable, connection);
            custservdb.ExecuteNonQuery();

            SqlCommand shiftdb = new SqlCommand(createShiftDimTable, connection);
            shiftdb.ExecuteNonQuery();
            SqlCommand custdmdb = new SqlCommand(createCustomerDimTable, connection);
            custdmdb.ExecuteNonQuery();
            SqlCommand empdimdb = new SqlCommand(createEmployeeDimTable, connection);
            empdimdb.ExecuteNonQuery();
            SqlCommand servtydimdb = new SqlCommand(createServiceTypeDimTable, connection);
            servtydimdb.ExecuteNonQuery();
            SqlCommand factdb = new SqlCommand(createCustomerServiceScheduleFactsTable, connection);
            factdb.ExecuteNonQuery();

        }
        
        public static void createTriggers(SqlConnection connection){
            string createCustomerTrigger = @"CREATE TRIGGER createOrUpdateCustomer
                                                ON customer
                                                FOR INSERT, UPDATE, DELETE
                                                AS
                                                BEGIN
                                                DECLARE @ins VARCHAR(255)
                                                DECLARE @del VARCHAR(255)
                                                DECLARE @id VARCHAR(255)
                                                DECLARE @name CHAR(20), @street CHAR(20), @city CHAR(20), @gender CHAR(20), @birthdate DATE
                                                DECLARE @Action VARCHAR(50)
                                                SELECT @ins = i.id, @id = i.id, @name = i.name, @street = i.street, @city = i.city, @gender = i.Gender, @birthdate = i.birthdate FROM inserted i
                                                SELECT @del = d.id, @id = d.id, @name = d.name FROM deleted d
                                                SET @Action = CASE WHEN EXISTS(SELECT * FROM inserted)
                                                                                AND EXISTS(SELECT * FROM deleted)
                                                                                THEN 'U'
                                                                                WHEN EXISTS(SELECT * FROM inserted)
                                                                                THEN 'I'
                                                                                WHEN EXISTS(SELECT * FROM deleted)
                                                                                THEN 'D'
                                                                                ELSE NULL
                                                                    END
                                                    IF @Action = 'I'
                                                    BEGIN
                                                        INSERT INTO customerdim (id, name, street, city, gender, birthdate, age) VALUES (@id, @name, @street, @city, @gender, @birthdate, DATEDIFF(hour,@birthdate,GETDATE())/8766)
                                                    END
                                                    ELSE IF @Action = 'U'
                                                    BEGIN
                                                        UPDATE customerdim SET name=@name, street=@street, city=@city, gender=@gender, birthdate=@birthdate, age=DATEDIFF(hour,@birthdate,GETDATE())/8766 WHERE id=@id
                                                    END
                                                    ELSE IF @Action = 'D'
                                                    BEGIN
                                                        DELETE FROM customerdim WHERE id=@id
                                                    END
                                                END";

            string createEmployeeTrigger = @"CREATE TRIGGER createOrUpdateEmployee
                                                ON employee
                                                FOR INSERT, UPDATE, DELETE
                                                AS
                                                BEGIN
                                                DECLARE @ins VARCHAR(255)
                                                DECLARE @del VARCHAR(255)
                                                DECLARE @id VARCHAR(255)
                                                DECLARE @name CHAR(20), @street CHAR(20), @city CHAR(20), @managerId INT, @jobTitle CHAR(20), @certification CHAR(100), @salary INT
                                                DECLARE @Action VARCHAR(50)
                                                SELECT @ins = i.id, @id = i.id, @name = i.name, @street = i.street, @city = i.city, @managerId = i.managerid, @jobTitle = i.JobTitle, @certification = i.Certification, @salary = i.Salary FROM inserted i
                                                SELECT @del = d.id, @id = d.id FROM deleted d
                                                SET @Action = CASE WHEN EXISTS(SELECT * FROM inserted)
                                                                                AND EXISTS(SELECT * FROM deleted)
                                                                                THEN 'U'
                                                                                WHEN EXISTS(SELECT * FROM inserted)
                                                                                THEN 'I'
                                                                                WHEN EXISTS(SELECT * FROM deleted)
                                                                                THEN 'D'
                                                                                ELSE NULL
                                                                    END
                                                    IF @Action = 'I'
                                                    BEGIN
                                                        INSERT INTO employeedim (ID, name, street, city, ManagerID, JobTitle, Certification, Salary) VALUES (@id, @name, @street, @city, @managerId, @jobTitle, @certification, @salary)
                                                    END
                                                    ELSE IF @Action = 'U'
                                                    BEGIN
                                                        UPDATE employeedim SET name=@name, street=@street, city=@city, ManagerID=@managerId, JobTitle=@jobTitle, Certification=@certification, Salary=@salary WHERE ID=@id
                                                    END
                                                    ELSE IF @Action = 'D'
                                                    BEGIN
                                                        DELETE FROM employeedim WHERE id=@id
                                                    END
                                                END";

            string createServiceTypeTrigger = @"CREATE TRIGGER createOrUpdateServiceType
                                                ON servicetype
                                                FOR INSERT, UPDATE, DELETE
                                                AS
                                                BEGIN
                                                DECLARE @ins VARCHAR(255)
                                                DECLARE @del VARCHAR(255)
                                                DECLARE @id VARCHAR(255)
                                                DECLARE @name CHAR(20), @certificationrqts CHAR(100), @rate INT
                                                DECLARE @Action VARCHAR(50)
                                                SELECT @ins = i.ID, @id = i.id, @name = i.name, @certificationrqts = i.CertificationRqts, @rate = i.Rate FROM inserted i
                                                SELECT @del = d.id, @id = d.id FROM deleted d
                                                SET @Action = CASE WHEN EXISTS(SELECT * FROM inserted)
                                                                                AND EXISTS(SELECT * FROM deleted)
                                                                                THEN 'U'
                                                                                WHEN EXISTS(SELECT * FROM inserted)
                                                                                THEN 'I'
                                                                                WHEN EXISTS(SELECT * FROM deleted)
                                                                                THEN 'D'
                                                                                ELSE NULL
                                                                    END
                                                    IF @Action = 'I'
                                                    BEGIN
                                                        INSERT INTO servicetypedim (ID, name, CertificationRqts, Rate) VALUES (@id, @name, @certificationrqts, @rate)
                                                    END
                                                    ELSE IF @Action = 'U'
                                                    BEGIN
                                                        UPDATE servicetypedim SET name=@name, CertificationRqts=@certificationrqts, Rate=@rate WHERE ID=@id
                                                    END
                                                    ELSE IF @Action = 'D'
                                                    BEGIN
                                                        DELETE FROM servicetypedim WHERE id=@id
                                                    END
                                                END";
        
            string createCustomerServiceScheduleTrigger = @"CREATE TRIGGER createOrUpdateCustomerServiceSchedule
                                                            ON customerServiceSchedule
                                                            FOR INSERT, UPDATE, DELETE
                                                            AS
                                                            BEGIN
                                                            DECLARE @ins VARCHAR(255)
                                                            DECLARE @del VARCHAR(255)
                                                            DECLARE @id VARCHAR(255)
                                                            DECLARE @cid VARCHAR(255), @stid VARCHAR(255), @empid VARCHAR(255), @exp_dur INT, @act_dur INT, @status char(20), @startdate date
                                                            DECLARE @shiftid TABLE (ID VARCHAR(255))
                                                            DECLARE @Action VARCHAR(50)
                                                            SELECT @cid = i.CustomerID, @stid = i.ServiceTypeID, @empid = i.EmployeeID, @exp_dur = i.ExpectedDuration, @act_dur = i.ActualDuration, @status = i.status, @startdate = i.StartDateTime FROM inserted i
                                                            SELECT @id = d.CustomerID, @empid = d.EmployeeID, @stid = d.ServiceTypeID FROM deleted d
                                                            SET @Action = CASE WHEN EXISTS(SELECT * FROM inserted)
                                                                                            AND EXISTS(SELECT * FROM deleted)
                                                                                            THEN 'U'
                                                                                            WHEN EXISTS(SELECT * FROM inserted)
                                                                                            THEN 'I'
                                                                                            WHEN EXISTS(SELECT * FROM deleted)
                                                                                            THEN 'D'
                                                                                            ELSE NULL
                                                                                END
                                                                IF @Action = 'I'
                                                                BEGIN
                                                                    INSERT INTO shiftdim (Name, WeekDay, MonthDay, Month, Quarter, Year) OUTPUT inserted.ID INTO @shiftid VALUES ('Morning', DATENAME(WEEKDAY, @startdate), DAY(@startdate), 
                                                                                            MONTH(@startdate), DATENAME(QUARTER,@startdate),YEAR(@startdate))
                                                                    INSERT INTO customerserviceschedulefacts (CustomerID, ServiceTypeID, EmployeeID, ShiftID, StartDateTime, ExpectedDurationTimeUnits, ActualDurationTimeUnits, status) 
                                                                                VALUES (@cid, @stid, @empid, (SELECT id FROM @shiftid), @startdate, @exp_dur, @act_dur, @status)
                                                                END
                                                                ELSE IF @Action = 'U'
                                                                BEGIN
                                                                    UPDATE customerserviceschedulefacts SET ExpectedDurationTimeUnits=@exp_dur, ActualDurationTimeUnits=@act_dur, status=@status WHERE CustomerID=@cid AND EmployeeID=@empid 
                                                                                            AND ServiceTypeID=@stid AND StartDateTime=@startdate
                                                                END
                                                                ELSE IF @Action = 'D'
                                                                BEGIN
                                                                    DELETE FROM customerserviceschedulefacts WHERE CustomerID=@cid AND EmployeeID=@empid AND ServiceTypeID=@stid
                                                                END
                                                            END";

            SqlCommand custTrigger = new SqlCommand(createCustomerTrigger, connection);
            custTrigger.ExecuteNonQuery();
            SqlCommand empTrigger = new SqlCommand(createEmployeeTrigger, connection);
            empTrigger.ExecuteNonQuery();
            SqlCommand servTrigger = new SqlCommand(createServiceTypeTrigger, connection);
            servTrigger.ExecuteNonQuery();
            SqlCommand cusServTrigger = new SqlCommand(createCustomerServiceScheduleTrigger, connection);
            cusServTrigger.ExecuteNonQuery();
        }

        private static void insertData(SqlConnection connection) {
            String data = "";
            if (File.Exists(textFile)){
                data = File.ReadAllText(textFile);
            }
            SqlCommand dataInsert = new SqlCommand(data, connection);
            dataInsert.ExecuteNonQuery();
        }

        public static void deleteData(SqlConnection connection) {
            String command = @"DELETE FROM CustomerServiceSchedule;
                        DELETE FROM employee;
                        DELETE FROM shiftdim;
                        DELETE FROM ServiceType;
                        DELETE FROM customer;";
            SqlCommand del = new SqlCommand(command, connection);
            del.ExecuteNonQuery();
        }

        static void Main(string[] args)
        {
            try 
            { 
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

                builder.DataSource = "127.0.0.1"; 
                builder.UserID = "sa";            
                builder.Password = "MyPass@word";     
                builder.InitialCatalog = "TestCSharp";
                builder.TrustServerCertificate=true;

                using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
                {
                    Console.WriteLine("\nQuery data example:");
                    Console.WriteLine("=========================================\n");
                    
                    connection.Open();       

                    // Method for deleting existing data
                    // deleteData(connection);

                    // Method for creating tables
                    // createTables(connection);

                    // Method for creating triggers
                    // createTriggers(connection);

                    // Method for inserting data
                    // insertData(connection);

                    String sql = "SELECT * FROM customer";

                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine(String.Format("{0}, {1}", reader[0], reader[1]));
                            }
                        }
                    }        


                    String customQuery = @"SELECT std.name, cd.gender, sd.year, SUM(std.rate * (csf.actualdurationtimeunits/60)) as Revenue
                                            FROM customerserviceschedulefacts csf
                                            INNER JOIN shiftdim sd ON csf.shiftid = sd.id
                                            INNER JOIN customerdim cd ON csf.customerid = cd.id
                                            INNER JOIN servicetypedim std ON csf.servicetypeid = std.id
                                            WHERE csf.status = 'Completed'
                                            GROUP BY 
                                                GROUPING SETS (
                                                    (std.name, cd.gender),
                                                    (sd.year),
                                                    ());";
                    
                    using (SqlCommand command1 = new SqlCommand(customQuery, connection)) {
                        using (SqlDataReader reader = command1.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine(String.Format("{0}, {1}", reader[0], reader[1]));
                            }
                        }
                    }
                    connection.Close();
                }
            }
            catch (SqlException e)
            {
                Console.WriteLine(e.ToString());
            }
            Console.WriteLine("\nDone. Press enter.");
            Console.ReadLine(); 

        }
    }
}