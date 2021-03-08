package connector;
import java.sql.*;
import java.io.*;

public class Main {

	public static void main(String[] args) throws Exception {

		try {
			Connection con= DriverManager.getConnection("jdbc:mysql://localhost:3306/Airline?serverTimezone=UTC", "root", "Steve#14n");
			int batchSize = 20;

			if (con !=null) {System.out.println("Connected");}
			String csvFilePath = "/Users/harrisonbueno/Desktop/NEIU/20Summer/DatabaseDesign/Project3/Flight.csv";
			String csvFilePath2 = "/Users/harrisonbueno/Desktop/NEIU/20Summer/DatabaseDesign/Project3/Airplane.csv";
			String csvFilePath3 = "/Users/harrisonbueno/Desktop/NEIU/20Summer/DatabaseDesign/Project3/Passengers.csv";
			String csvFilePath4 = "/Users/harrisonbueno/Desktop/NEIU/20Summer/DatabaseDesign/Project3/Booking.csv";


			con.setAutoCommit(false);

			///Upload table Airplane
            String sql = "insert into Airline.Airplane(AircraftMaker, AircraftModel, Capacity) VALUES (?, ?, ?)";
            PreparedStatement state2 = con.prepareStatement(sql);
            BufferedReader line = new BufferedReader(new FileReader(csvFilePath2));
            String lineText = null;

            int count = 0;
            line.readLine(); // skip header line

            while ((lineText = line.readLine()) != null) {
                String[] data = lineText.split(",");

                String AircraftMaker = data[0];
                String AircraftModel = data[1];
                String Capacity = data[2];

                state2.setString(1, AircraftMaker);
                state2.setString(2, AircraftModel);
                int x=Integer.parseInt(Capacity);
                state2.setInt(3, x);

                state2.addBatch();

                if (count % batchSize == 0) {
                    state2.executeBatch();
                }
            }

            line.close();
            state2.executeBatch();

            ////insert 2 table Flight
            sql = "INSERT INTO Airline.Flight (FlightDate, Carrier, FlightNum, Origin, Dest, DepTime, ArrTime, Model) VALUES (?, ?, ?, ?, ?, ?, ?,?)";
            PreparedStatement state = con.prepareStatement(sql);
			BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            lineText = null;

            count = 0;
            lineReader.readLine(); // skip header line

            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                String FlightDate = data[0];
                String Carrier = data[1];
                String FlightNum = data[2];
                String Origin = data[3];
                String Dest = data[4];
                String DepTime = data[5];
                String ArrTime = data[6];
                String Model = data[7];

                java.sql.Date sqlDate = Date.valueOf(FlightDate); ///insert Date Format sql
                state.setDate(1, sqlDate);
                state.setString(2, Carrier);
                int x= Integer.parseInt(FlightNum);
                state.setInt(3, x);
                state.setString(4, Origin);
                state.setString(5, Dest);
                x=Integer.parseInt(DepTime);
                state.setInt(6, x);
                x=Integer.parseInt(ArrTime);
                state.setInt(7, x);
                state.setString(8, Model);

                state.addBatch();

                if (count % batchSize == 0) {
                    state.executeBatch();
                }
            }

            lineReader.close();
            state.executeBatch();

            ////insert 3 Passengers
            sql = "INSERT INTO Airline.Passengers (name, email, DOB) VALUES (?, ?, ?)";
            PreparedStatement state3 = con.prepareStatement(sql);
			BufferedReader lineReader2 = new BufferedReader(new FileReader(csvFilePath3));
            lineText = null;

            count = 0;
            lineReader2.readLine(); // skip header line

            while ((lineText = lineReader2.readLine()) != null) {
                String[] data = lineText.split(",");
                String name = data[0];
                String email = data[1];
                String DOB = data[2];

                state3.setString(1, name);
                state3.setString(2, email);
                java.sql.Date sqlDate = Date.valueOf(DOB);
                state3.setDate(3, sqlDate);

                state3.addBatch();

                if (count % batchSize == 0) {
                    state3.executeBatch();
                }
            }

            lineReader2.close();
            state3.executeBatch();

            ////insert 4 Booking
            sql = "INSERT INTO Airline.Booking (bookingID, fd_id, fn_id, bookingDate, price, netPrice, Engine, activityDate, "
            		+"p_id, type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement state4 = con.prepareStatement(sql);
			BufferedReader lineReader3 = new BufferedReader(new FileReader(csvFilePath4));
            lineText = null;

            count = 0;
            lineReader3.readLine(); // skip header line

            while ((lineText = lineReader3.readLine()) != null) {
                String[] data = lineText.split(",");
                String bookID = data[0];   //int
                String fd = data[1];	//date
                String fn = data[2];	//int
                String bookD = data[3];	//date
                String price = data[4];	//float
                String netP = data[5];	//float
                String eng = data[6];	//string
                String actD = data[7];	//date
                String pid = data[8];	//string
                String type= data[9];	//string

                int x= Integer.parseInt(bookID);
                state4.setInt(1, x);   //1
                java.sql.Date sqlDate = Date.valueOf(fd); ///insert Date Format sql  2
                state4.setDate(2, sqlDate);
                x= Integer.parseInt(fn);
                state4.setInt(3, x);    //3
                sqlDate = Date.valueOf(bookD);
                state4.setDate(4, sqlDate);   ///4
                float y=Float.parseFloat(price);
                state4.setFloat(5,y);	  //5
                y=Float.parseFloat(netP);
                state4.setFloat(6, y);   //6
                state4.setString(7, eng); //7
                sqlDate = Date.valueOf(actD);
                state4.setDate(8, sqlDate);  //8
                state4.setString(9, pid);    //9
                state4.setString(10, type);  //10

                state4.addBatch();

                if (count % batchSize == 0) {
                    state4.executeBatch();
                }
            }

            lineReader3.close();
            state4.executeBatch();


            ////Close Connections
            con.commit();
			con.close();

		}catch(Exception e) {
			System.out.println(e);
		}
	}
}
