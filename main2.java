package Con;
import java.sql.*;
import java.util.Scanner;

public class Main {

	public static void print_table() {
		System.out.println("The following queries are available to execute:");
		String conditions="1) Total passengers by booking month. \n" +
							"2) Total passengers by departure month. \n" +
							"3) Plane models greater than average capacity \n" +
							"4) Min capacity plane \n" +
							"5) Select each model plane greater than avg capacity \n" +
							"6) Select origin airport by passengers\n" +
							"7) Select destinations airport by passengers \n"+
							"8) Select which Engine by airline pax \n" +
							"9) Select Passengers by activity date\n"+
							"10)Select Engines more expensive than average \n11)Press -1 to exit";
		System.out.println(conditions);
	}

	public static String print_q(int num){
		String q="";
		switch(num) {
		case 1:
			q="select date_format(B.bookingDate, '%M %Y') as bookMonth, count(P.name) as Pax " +
					"from Airline.Booking as B, Airline.Passengers as P where B.p_id=P.email group by bookMonth;";
			break;
		case 2:
			q="select date_format(B.fd_id, '%M %Y') as departureMonth, count(P.name) as Pax \n" +
					"from Airline.Booking as B, Airline.Passengers as P where P.email=B.p_id group by departureMonth";
			break;
		case 3:
			q="select AircraftMaker,AircraftModel, Capacity as TopCapacity from Airline.Airplane group by AircraftModel \n" +
					"having Capacity > (select avg(Capacity) from Airline.Airplane) order by TopCapacity desc";
			break;
		case 4:
			q="select AircraftMaker, AircraftModel, Capacity as LowestCapacity from Airline.Airplane " +
					"group by AircraftModel order by LowestCapacity asc limit 3";
			break;
		case 5:   ////change to greater than average nested
			q="select AircraftMaker, AircraftModel, Capacity from Airline.Airplane " +
					"where Capacity > (select avg(capacity) from Airline.Airplane)";
			break;
		case 6:
			q="select F.Origin as Origin, Count(B.p_id) as pax from Airline.Flight as F, Airline.Booking as B " +
					"where F.FlightNum=B.fn_id and F.FlightDate=B.fd_id group by Origin order by pax desc";
			break;
		case 7:
			q="select F.Dest as Destination, Count(B.p_id) as pax from Airline.Flight as F, Airline.Booking as B " +
					"where F.FlightNum=B.fn_id and F.FlightDate=B.fd_id group by Destination order by pax desc";
			break;
		case 8:
			q="select B.Engine as Engine, count(P.name) as pax from Booking as B, Passengers as P " +
					"where P.email=B.p_id group by Engine order by pax desc";
			break;
		case 9:
			q="select date_format(activityDate, '%M %Y') as ActivityDate, Count(p_id) as pax from Booking group by ActivityDate order by pax desc";
			break;
		case 10:
			q="select Engine, round(avg(price),2) as avgPrice from Booking group by Engine \n" +
					"having avg(price) > (select avg(price) from Booking) order by avgPrice";
			break;
		}

		return q;
	}

	public static void main(String[] args) throws Exception {

		try {
			Connection con= DriverManager.getConnection("jdbc:mysql://localhost:3306/Airline?serverTimezone=UTC", "root", /*PASSWORD */);
			if (con !=null) {
				System.out.println("Connected");
			}

			////Create table to output

			print_table();
			int num=0;
			Scanner scanner=new Scanner(System.in);
			boolean valid=true;
			//System.out.println("\nSelect a number from above options:");
			///start while loop

			while(valid) {
				System.out.println("\nSelect a number from above option menu:\n");
				num=scanner.nextInt();
				if(num>0 & num<11) {valid=true;}
				else if(num==-1) {valid=false;
				System.out.println("exit");}


			System.out.println("The query selected: "+num);

			String query=print_q(num);
			///create statement connection
			Statement stmt=con.createStatement();

			ResultSet rs=stmt.executeQuery(query);

			switch(num) {
			case 1 :
				System.out.printf("%15s %3s%n", "bookMonth", "Pax");
				while(rs.next()) {
					String form_date=rs.getString("bookMonth");
					int pax= rs.getInt("Pax");
					System.out.printf("%15s %3d%n", form_date, pax);
				}
				System.out.println();
				break;
			case 2:
				System.out.printf("%15s %3s%n", "departureMonth", "Pax");
				while(rs.next()) {
					String form_date=rs.getString("departureMonth");
					int pax= rs.getInt("Pax");
					System.out.printf("%15s %3d%n", form_date, pax);
				}
				System.out.println();
				break;
			case 3:
				System.out.printf("%10s %8s %8s%n", "AircraftMaker", "AirCraftModel", "TopCapacity");
				while(rs.next()) {
					String form_date=rs.getString("AircraftMaker");
					String model=rs.getString("AircraftModel");
					int pax= rs.getInt("TopCapacity");
					System.out.printf("%10s %10s %8d%n", form_date,model, pax);
				}
				System.out.println();
				break;
			case 4:
				System.out.printf("%10s %8s %8s%n", "AircraftMaker", "AirCraftModel", "LowestCapacity");
				while(rs.next()) {
					String form_date=rs.getString("AircraftMaker");
					String model=rs.getString("AircraftModel");
					int pax= rs.getInt("LowestCapacity");
					System.out.printf("%10s %10s %8d%n", form_date,model, pax);
				}
				System.out.println();
				break;
			case 5:
				System.out.printf("%10s %8s %8s%n", "AircraftMaker", "AirCraftModel", "Capacity");
				while(rs.next()) {
					String form_date=rs.getString("AircraftMaker");
					String model=rs.getString("AircraftModel");
					int pax= rs.getInt("Capacity");
					System.out.printf("%10s %10s %8d%n", form_date,model, pax);
				}
				System.out.println();
				break;
			case 6:
				System.out.printf("%8s %8s%n", "Origin", "pax");
				while(rs.next()) {
					String form_date=rs.getString("Origin");
					int pax= rs.getInt("pax");
					System.out.printf("%8s %8d%n", form_date,pax);
				}
				System.out.println();
				break;
			case 7:
				System.out.printf("%8s %8s%n", "Destination", "pax");
				while(rs.next()) {
					String form_date=rs.getString("Destination");
					int pax= rs.getInt("pax");
					System.out.printf("%8s %8d%n", form_date,pax);
				}
				System.out.println();
				break;
			case 8:
				System.out.printf("%8s %8s%n", "Engine", "pax");
				while(rs.next()) {
					String form_date=rs.getString("Engine");
					int pax= rs.getInt("pax");
					System.out.printf("%8s %8d%n", form_date,pax);
				}
				System.out.println();
				break;
			case 9:
				System.out.printf("%8s %8s%n", "ActivityDate", "pax");
				while(rs.next()) {
					String form_date=rs.getString("ActivityDate");
					int pax= rs.getInt("pax");
					System.out.printf("%8s %8d%n", form_date,pax);
				}
				System.out.println();
				break;
			case 10:
				System.out.printf("%8s %8s%n", "Engine", "avgPrice");
				while(rs.next()) {
					String form_date=rs.getString("Engine");
					int pax= rs.getInt("avgPrice");
					System.out.printf("%8s %8d%n", form_date,pax);
				}
				System.out.println();
				break;
				}


			}///exit while loop
			con.close();
			System.out.println("exit");


		}catch(Exception e) {
			System.out.println(e);
		}
	}


}
