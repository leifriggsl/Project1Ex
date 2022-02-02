//Author: Leif Riggs
//Date: 2/3/2022
//Purpose: To run administrative and user tasks for Music Song Statistics Application


import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner
import scala.collection.mutable.ArrayBuffer
//imported all the libraries
object Project1Ex {
    def main(args: Array[String]): Unit = {
        var adminChoice=""
        var scanner =new Scanner(System.in)
        // This block of code is all necessary for spark/hive/hadoop
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\") // change if winutils.exe is in a different bin folder
        val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Project1Ex")    // Change to whatever app name you want
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._

        //This block to connect to mySQL
        val driver = "com.mysql.cj.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/Demodatabase" // Modify for whatever port you are running your DB on
        val username = "root"
        val password = "FUzzy26!" // Update to include your password
        var connection:Connection = null

        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)

        // Method to check login credentials
        //***   val adminCheck = login(connection)
       //***   login(connection)
         var n=1 
         var arrBuff= new ArrayBuffer[Int]() 
         arrBuff+=0
         connection = DriverManager.getConnection(url, username, password)
         val statement4 = connection.createStatement()
         val resultSet4 = statement4.executeQuery("SELECT * FROM user_accounts")
        // log.write(Calendar.getInstance().getTimeInMillis + " - Excecuting 'SELECT * FROM Gradesheet;' \n")
         while ( resultSet4.next() ) //loading all user ID's into array buffer to get the next value
         {
          arrBuff+=  resultSet4.getString(1).toInt
         }
         n=arrBuff.max
         n += 1
         hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
                   hiveCtx.sql("SET hive.enforce.bucketing=false")
                   hiveCtx.sql("SET hive.enforce.sorting=false")
                //    hiveCtx.sql("DROP TABLE data1")
                 //   insertMusicData(hiveCtx:HiveContext)
         println("Welcome to this Music Trends Application.  Enter any string to continue.  Enter 'q' to quit.") //welcome message
         var enterProgram =scanner.nextLine()
         while (enterProgram!="q") //loop for if not ending the program
            {
              var adminCheck=false
              var userCheck=false //set both criteria to false to get a matching username and password
              while (adminCheck==false && userCheck==false) {
                 val statement = connection.createStatement()
                 val statement2 = connection.createStatement()
                 println("Enter username: ")
                 var scanner = new Scanner(System.in)
                 var username = scanner.nextLine().trim()
            
                 println("Enter password: ")
                 var password = scanner.nextLine().trim()
           
                 val resultSet = statement.executeQuery("SELECT COUNT(*) FROM admin_accounts WHERE username='"+username+"' AND password='"+password+"';")
                 while ( resultSet.next() ) {
                   if (resultSet.getString(1) == "1") {
                      adminCheck=true;
                      } //if admin username and password matches, set criteria for admin to true
                 }

            val resultSet2 = statement2.executeQuery("SELECT COUNT(*) FROM user_accounts WHERE username='"+username+"' AND password='"+password+"';")
            while ( resultSet2.next() ) {
                if (resultSet2.getString(1) == "1") {
                       userCheck=true;//if user username and password matches, set criteria for admin to true
                }
            }
            if (adminCheck==false && userCheck==false) //if no match, say to try again for the next loop
               {println("Username/password combo not found. Try again!")}
       }


        if (adminCheck==true) //enter admin area
        {        println("Welcome Admin!")
        
                 println("Please choose an administrative task as either '1', '2,' or '3'.  Enter '33' to enter user area.  Enter '-99' to quit")
                 println("1. Add new user as the last row in user table.")
                 println("2. Update a user's password.")
                 println("3. Delete a user from the table") //prompts for admin choices
                 var checker=false
                


            // try{
             
             
                 adminChoice = scanner.nextLine() //input for admin choice of 3 options

             
              //  throw new BadUserInputException
             //}
             //catch {
               //  case e: BadUserInputException =>{println("Bad Input"); e.printStackTrace() }
            // }
            // finally{}
             
              //  var runAdmin=scanner.nextLine()


                while (adminChoice!=("-99")) //enter loop for executing admin choice if it is 1, 2, or 3
                {

                   

                  if (adminChoice == "1") //this is the choice to add a new username and password account to the admin table
                  {
                    connection = DriverManager.getConnection(url, username, password)
                    val statement4 = connection.createStatement()
                    val resultSet4 = statement4.executeQuery("SELECT * FROM user_accounts")
                    // log.write(Calendar.getInstance().getTimeInMillis + " - Excecuting 'SELECT * FROM Gradesheet;' \n")
                    arrBuff.clear()
                    arrBuff+=0
                    while ( resultSet4.next() ) 
                     {
                        arrBuff+=  resultSet4.getString(1).toInt
                     }
                    n=arrBuff.max
                    n += 1     //clear and update array buffer to give the next user id if we are adding an account to the admin table
                    println("Please enter a new username: ")
                    var newUserName = scanner.nextLine()
                    newUserName="'" +newUserName + "'"
                    println("Please enter a new password: ")
                    var newPassword=scanner.nextLine()
                    newPassword="'" + newPassword + "'"
                    connection = DriverManager.getConnection(url, username, password)
                    val statement2 = connection.createStatement()
                    val resultSet2 = statement2.executeUpdate("INSERT INTO user_accounts(userID, username, password) VALUES ("+n+", "+newUserName+", "+newPassword+")" ) //inserting into MySQL admin table new username and password
                    //  log.write(Calendar.getInstance().getTimeInMillis  + " -  Executing 'INSERT INTO Gradesheet(StudentNumber, StudentName, Period, Grade) VALUES ("+n+", "+inputName+", "+inputPeriod+", "+inputScore+");' \n")
                    println() 
                    connection = DriverManager.getConnection(url, username, password)
                    val statement6 = connection.createStatement() //printing out the admin table with new data
                    val resultSet6 = statement6.executeQuery("SELECT * FROM user_accounts")
                    while ( resultSet6.next() ) {
                        print(resultSet6.getString(1) + " " + resultSet6.getString(2) + " " + resultSet6.getString(3))
                        println()
                    }
                     //   connection = DriverManager.getConnection(url, username, password)
                       //  val statement5 = connection.createStatement()
                        //val resultSet5 = statement5.executeQuery("SELECT * FROM user_accounts")
                    }    
                     //log.write(Calendar.getInstance().getTimeInMillis + " - Excecuting 'SELECT * FROM Gradesheet;' \n")
                else if (adminChoice=="2") //this is the choice to update a username and password based on the user ID in the table
                {
                    connection = DriverManager.getConnection(url, username, password) 
                    val statement4 = connection.createStatement()
                    val resultSet4 = statement4.executeQuery("SELECT * FROM user_accounts")
                    // log.write(Calendar.getInstance().getTimeInMillis + " - Excecuting 'SELECT * FROM Gradesheet;' \n")
                    arrBuff.clear()
                    arrBuff+=0
                    while ( resultSet4.next() ) 
                    {
                      arrBuff+=  resultSet4.getString(1).toInt
                    }
                    n=arrBuff.max
                    n += 1 //clear and update array buffer so that the next user ID will be correct
                    println("The User table is as follows.  Please choose a userID whose user name and password will be updated")

                    connection = DriverManager.getConnection(url, username, password) 
                    val statement6 = connection.createStatement()
                    val resultSet6 = statement6.executeQuery("SELECT * FROM user_accounts")
                    while ( resultSet6.next() ) {
                              print(resultSet6.getString(1) + " " + resultSet6.getString(2) + " " + resultSet6.getString(3))
                              println()
                           }//print the table so that we know which User ID we want to choose

                    var chooseUserUpdate=scanner.nextInt() //user input for which account to update
                    scanner.nextLine()
                    for(i <-0 to arrBuff.length-1)
                      {
                        if (chooseUserUpdate==arrBuff(i))
                           {checker=true}
                      }
                    if (checker==true)
                    {  println("Please enter this user's user name password") //entering new username
                       var updateUserName=scanner.nextLine()
                       updateUserName="'" + updateUserName + "'"
                       connection = DriverManager.getConnection(url, username, password)
                       val statement15 = connection.createStatement()
                       val resultSet15 = statement15.executeUpdate("UPDATE user_accounts SET username= "+updateUserName+" WHERE userID= "+chooseUserUpdate+" " )
                        
                       println("Please enter this user's new password") //entering new password
                       var updatePassword=scanner.nextLine()
                       updatePassword="'" + updatePassword + "'"
                       connection = DriverManager.getConnection(url, username, password)
                       val statement7 = connection.createStatement()
                       val resultSet7 = statement7.executeUpdate("UPDATE user_accounts SET password= "+updatePassword+" WHERE userID= "+chooseUserUpdate+" " )
                       println("The User table is as follows.")
                       connection = DriverManager.getConnection(url, username, password)
                       val statement16 = connection.createStatement()
                       val resultSet16 = statement16.executeQuery("SELECT * FROM user_accounts")
                       while ( resultSet16.next() ) {
                          print(resultSet16.getString(1) + " " + resultSet16.getString(2) + " " + resultSet16.getString(3))
                          println()
                        }//print the updated table
                       checker=false
                    }
                    else{println("This user ID is not in the table")}

                }
                else if (adminChoice=="3") //choice if we are deleting an account
                {
                  connection = DriverManager.getConnection(url, username, password)
                  val statement4 = connection.createStatement()
                  val resultSet4 = statement4.executeQuery("SELECT * FROM user_accounts")
                  // log.write(Calendar.getInstance().getTimeInMillis + " - Excecuting 'SELECT * FROM Gradesheet;' \n")
                  arrBuff.clear()
                  arrBuff+=0
                  while ( resultSet4.next() ) 
                  {
                    arrBuff+=  resultSet4.getString(1).toInt
                  }
                  n=arrBuff.max
                  n += 1  //clear and update array buffer so we get the correct next user ID
                  println("The User table is as follows.  Please choose a userID whose account will be deleted")
                  connection = DriverManager.getConnection(url, username, password)
                  val statement17 = connection.createStatement()
                  val resultSet17 = statement17.executeQuery("SELECT * FROM user_accounts")
                  while ( resultSet17.next() ) {
                    print(resultSet17.getString(1) + " " + resultSet17.getString(2) + " " + resultSet17.getString(3))
                    println()
                  } //print the table so we know which account we want to delete
                  var chooseUserDelete=scanner.nextInt()
                  scanner.nextLine()//input for deleting an account
                  var checker2=false
                  for(i <-0 to arrBuff.length-1)
                  {
                    if (chooseUserDelete==arrBuff(i))
                        {checker2=true}
                    }

                  if (checker2==true)
                  {  connection = DriverManager.getConnection(url, username, password)
                     val statement10 = connection.createStatement()
                     val resultSet10 = statement10.executeUpdate("DELETE FROM user_accounts WHERE userID = "+chooseUserDelete+" ") //delete account
                     //  log.write(Calendar.getInstance().getTimeInMillis + " - Excecuting 'DELETE FROM Gradesheet WHERE StudentNumber = "+n+"' \n")
                     println("The User table is as follows.")
                     connection = DriverManager.getConnection(url, username, password)
                     val statement18 = connection.createStatement()
                     val resultSet18 = statement18.executeQuery("SELECT * FROM user_accounts")
                     while ( resultSet18.next() ) {
                        print(resultSet18.getString(1) + " " + resultSet18.getString(2) + " " + resultSet18.getString(3))
                        println()
                     } //print the table after deleting account
                  }
                 else {println("This user ID is not in the table")}  
                }
                  
                else if (adminChoice==("33")) //choice for admin to enter user area
                {
                    println("Please enter a user task as '1', '2', '3', '4', '5', or '6'.  Enter '-99' to return to Admin area") //this is all the choices that the user has for querying the data set
                    println("1. View the 20 most popular songs sorted for only one song per artist")
                    println("2. Songs that are scored greater than .7 for both Danceability and Energy")
                    println("3. Songs that are scored greater than .7 for both Danceability and Energy, but with tempo less than 100")
                    println("4. Popular songs by the Arctic Monkeys")
                    println("5. Popular songs by the Selena Gomez (if they exist in the database)")
                    println("6. Fastest songs")
           
                    var  userChoice=scanner.nextLine()
                  
                    while(userChoice!=("-99")) // if we enter one of 6 choices, we will run that method
                    {
                        if(userChoice=="1")
                        {mostPopTrack(hiveCtx:HiveContext)}

                        else if (userChoice=="2")
                        {danceableAndEnergetic(hiveCtx:HiveContext)}

                        else if (userChoice=="3")
                        {danceableEnergeticAndSlow(hiveCtx:HiveContext)}

                        else if (userChoice=="4")            
                        {Arctic(hiveCtx:HiveContext)}
           
                        else if (userChoice=="5")
                        {Selena(hiveCtx:HiveContext)}
                        
                        else if (userChoice=="6")
                        {fastest(hiveCtx:HiveContext)}
                        //  else if (userChoice=="7")
                        //    {}

                        else {println("This is not a user task. Please enter a user task as '1', '2', '3', '4', '5', or '6'.  Enter '-99' to quit") 
                             println("1. View the 20 most popular songs sorted for only one song per artist")
                             println("2. Songs that are scored greater than .7 for both Danceability and Energy")
                             println("3. Songs that are scored greater than .7 for both Danceability and Energy, but with tempo less than 100")
                             println("4. Popular songs by the Arctic Monkeys")
                             println("5. Popular songs by the Selena Gomez (if they exist in the database)")
                             println("6. Fastest songs")
                        } //if we didn't enter 1-6 it says that it is not a user task for re-entering a choice
                        if (userChoice=="1" ||userChoice=="2" ||userChoice=="3"||userChoice=="4"||userChoice=="5"||userChoice=="6")//if we entered 1-6 last time, we reprint the prompts for user choices for the next loop 
                            {println("Please enter a user task as '1', '2', '3', '4', '5', or '6'.  Enter '-99' to quit")
                            println("1. View the 20 most popular songs sorted for only one song per artist")
                            println("2. Songs that are scored greater than .7 for both Danceability and Energy")
                            println("3. Songs that are scored greater than .7 for both Danceability and Energy, but with tempo less than 100")
                            println("4. Popular songs by the Arctic Monkeys")
                            println("5. Popular songs by the Selena Gomez (if they exist in the database)")
                            println("6. Fastest songs")
                        }   
                        userChoice=scanner.nextLine() //next input for next loop for user choices
              
                     }

                } // this is the end of the area where the admin can do user tasks


        else {println("This is not an administrative task")} //this is if the admin did not enter an administrative task when we first entered the admin area


                  
        
             println("Please choose an administrative task as either '1', '2,' or '3'.  Enter '33' to do user tasks as the Admin. Enter '-99' to quit")
             adminChoice=scanner.nextLine() //enter the next admin task
                

        } //end of admin task while loop

    } //end of admin area


       

        
        
        
        
        
    if (userCheck==true) //if username and password match a user account
        { println("Please enter a user task as '1', '2', '3', '4', '5', or '6'.  Enter '-99' to quit. Enter '77' to update your username and password")
          println("1. View the 20 most popular songs sorted for only one song per artist")
          println("2. Songs that are scored greater than .7 for both Danceability and Energy")
          println("3. Songs that are scored greater than .7 for both Danceability and Energy, but with tempo less than 100")
          println("4. Popular songs by the Arctic Monkeys")
          println("5. Popular songs by the Selena Gomez (if they exist in the database)")
          println("6. Fastest songs")
          var  userChoice=scanner.nextLine() //input for user task
                   
          while(userChoice!=("-99")) //Loop for running whichever task 1-6 that the user has chosen. We run the method of the task chosen in this loop
            {
               if(userChoice=="1")
                {mostPopTrack(hiveCtx:HiveContext)}

               else if (userChoice=="2")
                {danceableAndEnergetic(hiveCtx:HiveContext)}

                else if (userChoice=="3")
                    {  danceableEnergeticAndSlow(hiveCtx:HiveContext)}
                
                else if (userChoice=="4")            
                    {Arctic(hiveCtx:HiveContext)}
                
                else if (userChoice=="5")
                    {Selena(hiveCtx:HiveContext)}
                
                else if (userChoice=="6")
                {fastest(hiveCtx:HiveContext)}
        else if (userChoice=="77")
            {

                println("Please enter your new username.") //entering new username
                var updateUserName2=scanner.nextLine()
                updateUserName2="'" + updateUserName2 + "'"
                var  password2="'"+password+"'"
                var    username2="'"+username+"'"
                connection = DriverManager.getConnection(url, username, password)
                val statement20 = connection.createStatement()
                val resultSet20 = statement20.executeUpdate("UPDATE user_accounts SET username= "+updateUserName2+" WHERE username= "+username2+" " )
                println("Please enter your new password") //entering new password
                var updatePassword2=scanner.nextLine()
                updatePassword2="'" + updatePassword2 + "'"
                connection = DriverManager.getConnection(url, username, password)
                val statement21 = connection.createStatement()
                val resultSet21 = statement21.executeUpdate("UPDATE user_accounts SET password= "+updatePassword2+" WHERE password= "+password2+" " )
                println("Your username and password have been updated")

            }


        else {println("This is not a user task. Please enter a user task as '1', '2', '3', '4', '5', or '6'.  Enter '-99' to quit")
             println("1. View the 20 most popular songs sorted for only one song per artist")
             println("2. Songs that are scored greater than .7 for both Danceability and Energy")
             println("3. Songs that are scored greater than .7 for both Danceability and Energy, but with tempo less than 100")
             println("4. Popular songs by the Arctic Monkeys")
             println("5. Popular songs by the Selena Gomez (if they exist in the database)")
             println("6. Fastest songs") //this prompt is for if the user did not choose 1-6
            }
        if (userChoice=="1" ||userChoice=="2" ||userChoice=="3"||userChoice=="4"||userChoice=="5"||userChoice=="6"||userChoice=="77")
            {   println("Please enter a user task as '1', '2', '3', '4', '5', or '6'.  Enter '-99' to quit")
                println("1. View the 20 most popular songs sorted for only one song per artist")
                println("2. Songs that are scored greater than .7 for both Danceability and Energy")
                println("3. Songs that are scored greater than .7 for both Danceability and Energy, but with tempo less than 100")
                println("4. Popular songs by the Arctic Monkeys")
                println("5. Popular songs by the Selena Gomez (if they exist in the database)")
                println("6. Fastest songs") //this is for the prompts for the next user choice
            }
        userChoice=scanner.nextLine() // input for the next user choice
                    
    
    }//end of loop for making a user choice
        
        
        
        } //end of user area
        

        // Run method to insert Covid data. Only needs to be ran initially, then table data1 will be persisted.
        
        
      
      
        /*
        * Here is where I would ask the user for input on what queries they would like to run, as well as
        * method calls to run those queries. An example is below, top10DeathRates(hiveCtx) 
        * 
        */

      //  top10DeathRates(hiveCtx)
       println("Please Enter any string to continue.  Enter 'q' to quit.") //back to area where we first enter so that we can quit the program if we want
        enterProgram =scanner.nextLine()
 }
        sc.stop() // Necessary to close cleanly. Otherwise, spark will continue to run and run into problems.
   
    }

    // This method checks to see if a user-inputted username/password combo is part of a mySQL table.
    // Returns true if admin, false if basic user, gets stuckl in a loop until correct combo is inputted (FIX)
    def login(connection: Connection): Unit = { //method that can be used for login for admin or user
       var adminCheck=false
       var userCheck=false 
       while (adminCheck==false && userCheck==false) {
       val statement = connection.createStatement()
       val statement2 = connection.createStatement()
       println("Enter username: ")
       var scanner = new Scanner(System.in)
       var username = scanner.nextLine().trim()
            
       println("Enter password: ")
       var password = scanner.nextLine().trim()
           
       val resultSet = statement.executeQuery("SELECT COUNT(*) FROM admin_accounts WHERE username='"+username+"' AND password='"+password+"';")
       while ( resultSet.next() ) {
                if (resultSet.getString(1) == "1") {
                    adminCheck=true;
                }
            }

            val resultSet2 = statement2.executeQuery("SELECT COUNT(*) FROM user_accounts WHERE username='"+username+"' AND password='"+password+"';")
            while ( resultSet2.next() ) {
                if (resultSet2.getString(1) == "1") {
                    userCheck=true;
                }
            }
            if (adminCheck==false && userCheck==false)
            {println("Username/password combo not found. Try again!")}
        }
       
    }

    def insertMusicData(hiveCtx:HiveContext): Unit = { //method for inserting my csv file into my table
                //hiveCtx.sql("LOAD DATA LOCAL INPATH 'input/covid_19_data.txt' OVERWRITE INTO TABLE data1")
        //hiveCtx.sql("INSERT INTO data1 VALUES (1, 'date', 'California', 'US', 'update', 10, 1, 0)")

        // This statement creates a DataFrameReader from your file that you wish to pass in. We can infer the schema and retrieve
        // column names if the first row in your csv file has the column names. If not wanted, remove those options. This can 
        // then be 
       val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/train1.csv")
        output.limit(15).show() // Prints out the first 15 lines of the dataframe

        // output.registerTempTable("data2") // This will create a temporary table from your dataframe reader that can be used for queries. 

        // These next three lines will create a temp view from the dataframe you created and load the data into a permanent table inside
        // of Hadoop. Thus, we will have data persistence, and this code only needs to be ran once. Then, after the initializatio, this 
        // code as well as the creation of output will not be necessary.
  
        output.createOrReplaceTempView("temp_data")
       hiveCtx.sql("CREATE TABLE IF NOT EXISTS data1 (Artist_name STRING, Track_Name STRING, Popularity INT, Danceability FLOAT, Energy FLOAT, Key INT, Loudness FLOAT, Mode INT, Speechiness FLOAT, Acousticness FLOAT, Instrumental FLOAT, Liveness FLOAT, Valence FLOAT, Tempo FLOAT, Duration FLOAT, Time_Sign INT) PARTITIONED BY (name STRING) CLUSTERED BY (Popularity) INTO 4 BUCKETS row format delimited fields terminated by ',' stored as textfile TBLPROPERTIES(\"skip.header.line.count\"=\"1\")")
      //   hiveCtx.sql("CREATE TABLE IF NOT EXISTS data1 (name STRING, Track_Name STRING, Popularity INT, Danceability FLOAT, Energy FLOAT, Key INT, Loudness FLOAT, Mode INT, Speechiness FLOAT, Acousticness FLOAT, Instrumental FLOAT, Liveness FLOAT, Valence FLOAT, Tempo FLOAT, Duration FLOAT, Time_Sign INT, Class INT)")
        hiveCtx.sql("INSERT INTO data1 SELECT * FROM temp_data")
        
        // To query the data1 table. When we make a query, the result set ius stored using a dataframe. In order to print to the console, 
        // we can use the .show() method.
        val summary = hiveCtx.sql("SELECT * FROM data1 LIMIT 10")
        summary.show()   
    }

def mostPopTrack(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT DISTINCT(Track_Name), Popularity FROM data1 ORDER BY Popularity DESC")
        result.show()
} //method for most popular songs
def danceableAndEnergetic(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT DISTINCT(Track_Name), Danceability, Energy FROM data1 WHERE Danceability>.7 AND Energy>.7 ORDER BY Danceability DESC")
        result.show()} //method for danceable and energetic songs
 def danceableEnergeticAndSlow(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT DISTINCT(Track_Name), Danceability, Energy, Tempo FROM data1 WHERE Danceability>.7 AND Energy>.7 AND Tempo<100 ORDER BY Danceability DESC")
        result.show()
        }//method for danceable, energetic, an slow songs

def Arctic(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT Track_Name, Popularity FROM data1 WHERE Track_Name='Do I Wanna Know' ORDER BY Popularity DESC")
        result.show()
        }//method to see if Arctic Monkeys song is in table
def Selena(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT DISTINCT(name), Track_Name, Popularity FROM data1 WHERE name='Selena Gomez' ORDER BY Popularity DESC")
        result.show()
        }//method to see if Selena Gomez is in table
def fastest(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT DISTINCT(Track_Name), Danceability, Energy, Tempo FROM data1 WHERE Tempo >100 ORDER BY Tempo DESC")
        result.show()
        } //method to find the fastest songs

        //result.write.csv("results/mostPopTrack") 
     

  
}//end of class





