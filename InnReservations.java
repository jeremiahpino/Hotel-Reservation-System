// imports needed
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import javax.naming.NameAlreadyBoundException;

import java.util.LinkedHashMap;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.ArrayList;
import java.time.*;
import java.util.Date;

public class InnReservations {
    
    public static void main(String[] args) {
        try {
            InnReservations hp = new InnReservations();
                int functionalRequirement = Integer.parseInt(args[0]);
                
                switch (functionalRequirement) {
                case 1: hp.fr1(); break;
                case 2: hp.fr2(); break;
                case 3: hp.fr3(); break;
                case 4: hp.fr4(); break;
                case 5: hp.fr5(); break;
                case 6: hp.fr6(); break;
                }
                
        } catch (SQLException e) {
            System.err.println("SQLException: " + e.getMessage());
        } catch (Exception e2) {
                System.err.println("Exception: " + e2.getMessage());
            }
        }

    // room popularity score
    private void fr1() throws SQLException {

        System.out.println("FR1: Popular Rooms.\r\n");
        
	    // Step 1: Establish connection to RDBMS
	    try (Connection conn = DriverManager.getConnection(System.getenv("HP_JDBC_URL"),
							   System.getenv("HP_JDBC_USER"),
							   System.getenv("HP_JDBC_PW"))) {
	    // Step 2: Construct SQL statement
	    String sql = """
            -- select columns and do computations
            select lab7_rooms.RoomCode, lab7_rooms.RoomName, lab7_rooms.Beds, lab7_rooms.bedType, lab7_rooms.maxOcc, lab7_rooms.basePrice, lab7_rooms.decor, ROUND(ABS(sum(DATEDIFF( lab7_reservations.Checkin, lab7_reservations.Checkout ))) / 180, 2) AS "PopularScore", max(lab7_reservations.Checkout) as "NextCheckin", ABS(DATEDIFF( max(lab7_reservations.Checkin), max(lab7_reservations.Checkout) ) ) as "DaysOfMostRecentStay"
            -- from joined tables below
            from lab7_rooms
            join lab7_reservations on lab7_rooms.RoomCode = lab7_reservations.Room
            where (lab7_reservations.Checkin between date_sub(curdate(), interval 180 day) and curdate()) or (lab7_reservations.Checkout between date_sub(curdate(), interval 180 day) and curdate())
            -- group by each distinct room 
            group by lab7_rooms.RoomCode
            -- order by days occupied in descending order
            order by PopularScore desc
            
                """;

	    // Step 3: (omitted in this example) Start transaction

	    // Step 4: Send SQL statement to DBMS
	    try (Statement stmt = conn.createStatement();
		 ResultSet rs = stmt.executeQuery(sql)) {

		// Step 5: Receive results
		while (rs.next()) {
		    String RoomCode = rs.getString("RoomCode");
		    String RoomName = rs.getString("RoomName");

            String Beds = rs.getString("Beds");
		    String bedType = rs.getString("bedType");
            String maxOcc = rs.getString("maxOcc");
		    String basePrice = rs.getString("basePrice");
            String decor = rs.getString("decor");


		    float PopularScore = rs.getFloat("PopularScore");
            String nextCheckin = rs.getString("nextCheckin");
            String DaysOfMostRecentStay = rs.getString("DaysOfMostRecentStay");
		    System.out.format("%s %s %s %s %s %s %s ($%.2f) %s %s %n", RoomCode, RoomName, Beds, bedType,
            maxOcc, basePrice, decor, 
            PopularScore, nextCheckin, DaysOfMostRecentStay);
		}
	    }

	    
	}
	// Step 7: Close connection (handled by try-with-resources syntax)
    }

    // room to book according to user preference    
    private void fr2() throws SQLException {

        System.out.println("FR2: Available Rooms to Book.\r\n");
        
		// Step 1: Establish connection to RDBMS
		try (Connection conn = DriverManager.getConnection(System.getenv("HP_JDBC_URL"),
							   System.getenv("HP_JDBC_USER"),
							   System.getenv("HP_JDBC_PW"))) {
	    
        
        // Step 2: Construct SQL statement
        
        // ask for user information to potentially book room
	    Scanner scanner = new Scanner(System.in);
	    System.out.print("Enter a first name: ");
	    String firstName = scanner.nextLine();

	    System.out.format("Enter a last name: ");
        String lastName = scanner.nextLine();

        System.out.format("Enter a room code or 'any' for all rooms: ");
        String roomCode = scanner.nextLine();

        System.out.format("Enter a desired bed type or 'any' for all bed types: ");
        String bedType = scanner.nextLine();

        System.out.format("Enter a begin date (YYYY-MM-DD): ");
        LocalDate beginDate = LocalDate.parse(scanner.nextLine());

        System.out.format("Enter an end date (YYYY-MM-DD): ");
        LocalDate endDate = LocalDate.parse(scanner.nextLine());

        System.out.format("Enter number of children: ");
        Integer numChildren = Integer.valueOf(scanner.nextLine());

        System.out.format("Enter number of adults: ");
        Integer numAdults = Integer.valueOf(scanner.nextLine());

        List<Object> params = new ArrayList<Object>();
        //params.add(firstName);
        //params.add(lastName);

        // might need this when adding params java.sql.Date.valueOf(beginDate)
        params.add(beginDate);
        params.add(endDate);

        // second set of begin and end dates
        params.add(beginDate);
        params.add(endDate);

        Integer maxOccupancy = numChildren + numAdults;
        params.add(maxOccupancy);

        // string for sql statement 
	    StringBuilder sb = new StringBuilder( """
            select lab7_rooms.RoomName -- , Checkin, Checkout
            from lab7_rooms
            join lab7_reservations on lab7_rooms.RoomCode = lab7_reservations.Room
            where (Checkin not between ? and ?) and (Checkout not between ? and ?) and maxOcc >= ?
                """
            );

	    if (!"any".equalsIgnoreCase(roomCode)) {
		    sb.append(" AND RoomCode = ?");
		    params.add(roomCode);
	    }

        if (!"any".equalsIgnoreCase(bedType)) {
		    sb.append(" AND bedType = ?");
		    params.add(bedType);
	    }

        sb.append("group by lab7_rooms.RoomName");

        if(maxOccupancy > 4) {
            System.out.println("No suitable rooms are available.");
        }

        // list of rows
	    List<String> rowVals = new ArrayList<String>();

	    try (PreparedStatement pstmt = conn.prepareStatement(sb.toString())) {
            int i = 1;
            for (Object p : params) {
                pstmt.setObject(i++, p);
            }

            try (ResultSet rs = pstmt.executeQuery()) {

                int bookingNum = 1;
                // result set is empty (no exact reservation matches)
                if (!rs.isBeforeFirst() ) {    
                    System.out.println("No data"); 

                    try (PreparedStatement pstmt2 = conn.prepareStatement("select lab7_rooms.RoomName from lab7_rooms join lab7_reservations on lab7_rooms.RoomCode = lab7_reservations.Room where (Checkin not between ? and ?) and (Checkout not between ? and ?) and maxOcc >= ? group by lab7_rooms.RoomName limit 5") ) {

                        pstmt2.setDate(1, java.sql.Date.valueOf(beginDate));
                        pstmt2.setDate(2, java.sql.Date.valueOf(endDate));
                        pstmt2.setDate(3, java.sql.Date.valueOf(beginDate));
                        pstmt2.setDate(4, java.sql.Date.valueOf(endDate));
                        pstmt2.setInt(5, maxOccupancy);
                        
                        try( ResultSet rs2 = pstmt2.executeQuery() ) {
                            System.out.println("Rooms Available:");
                            while (rs2.next()) {
                            System.out.format("%s" + " "+ bookingNum++ +  "%n", rs2.getString("RoomName"));
                            rowVals.add(rs2.getString("RoomName"));
                            }
                            
                        }

                    }

                } 
                else {
                    System.out.println("Rooms Available:");
                    while (rs.next()) {
                    System.out.format("%s " + " "+ bookingNum++ + "%n", rs.getString("RoomName"));
                    rowVals.add(rs.getString("RoomName"));
                    }
                }
            }

            }

            // select num to book
            System.out.print("Select a booking number or type cancel to cancel current request: ");
	        String sBookNum = scanner.nextLine();

            // check for cancel
            if( sBookNum == "cancel" || sBookNum == "Cancel") {
                System.out.println("Cancelled current request.");
                System.exit(0);
            }

            // convert string to integer
            int bookNum = Integer.parseInt(sBookNum);
            // System.out.println(bookNum);
            
            String roomToBook = rowVals.get(bookNum - 1);

            // System.out.println(roomToBook);

            String sql = "select * from lab7_rooms where lab7_rooms.RoomName = '" + roomToBook + "'";

            String iRoomCode = "";
            String iRoomName = "";
     
            String iBeds = "";
            String ibedType2 = "";
            String imaxOcc = "";
            String ibasePrice = "";
            String idecor = "";

             // Step 4: Send SQL statement to DBMS
	        try (Statement stmt = conn.createStatement();
             ResultSet rs3 = stmt.executeQuery(sql)) {

                // Step 5: Receive results
                while (rs3.next()) {
                    String RoomCode = rs3.getString("RoomCode");
                    String RoomName = rs3.getString("RoomName");

                    String Beds = rs3.getString("Beds");
                    String bedType2 = rs3.getString("bedType");
                    String maxOcc = rs3.getString("maxOcc");
                    String basePrice = rs3.getString("basePrice");
                    String decor = rs3.getString("decor");

                    // System.out.format("%s %s %s %s %s %s %s %n", RoomCode, RoomName, Beds, bedType2,
                    // maxOcc, basePrice, decor);

                    iRoomCode = RoomCode;
                    iRoomName = RoomName;
                    iBeds = Beds;
                    ibedType2 = bedType2;
                    imaxOcc = maxOcc;
                    ibasePrice = basePrice;
                    idecor = decor;
                }
            }

        // days in between begin and end date
        long numBetweenDays = ChronoUnit.DAYS.between(beginDate, endDate);
        double rateP = Double.parseDouble(ibasePrice);
        double numBetDays = (double)numBetweenDays;
        // System.out.println("here1");
        Double stayRate = numBetDays * rateP;
        // System.out.println("here2");

        // show booking details
        System.out.format("Booking Details: %s, %s, %s, %s, %s, " + beginDate + ", " + endDate + ", " + numAdults + ", " + numChildren + ", " + stayRate + "%n", firstName, lastName
        , iRoomCode, iRoomName, ibedType2);
        
        // confirm booking
        System.out.print("Confirm or cancel this booking: ");
        String confirmation = scanner.nextLine();

        // check for cancel
        if( confirmation == "cancel" || confirmation == "Cancel") {
            System.out.println("Booking Cancelled.");
            System.exit(0);
        }

	    String updateSql = "insert into lab7_reservations(CODE, Room, Checkin, Checkout, Rate, LastName, FirstName, Adults, Kids) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Random rnd = new Random();
        int number = rnd.nextInt(999999);
    
        // this will convert any number sequence into 6 character.
        String iCode = String.format("%06d", number);

	    // Step 3: Start transaction
	    conn.setAutoCommit(false);
	    
	    try (PreparedStatement pstmtInsert = conn.prepareStatement(updateSql)) {
	
            // Step 4: Send SQL statement to DBMS
            pstmtInsert.setString(1, iCode);
            pstmtInsert.setString(2, iRoomCode);
            pstmtInsert.setDate(3, java.sql.Date.valueOf(beginDate));
            pstmtInsert.setDate(4, java.sql.Date.valueOf(endDate));
            pstmtInsert.setString(5, ibasePrice);
            pstmtInsert.setString(6, lastName);
            pstmtInsert.setString(7, firstName);
            pstmtInsert.setInt(8, numAdults);
            pstmtInsert.setInt(9, numChildren);
            
            int rowCount = pstmtInsert.executeUpdate();
		
            // Step 5: Handle results
            System.out.format("Inserted %d reservation for %s %s.%n", rowCount, firstName,lastName);

		// Step 6: Commit or rollback transaction
		conn.commit();
	    } catch (SQLException e) {
		conn.rollback();
	    }
        // Step 7: Close connection (handled implcitly by try-with-resources syntax)
	}
    }

    // change a reservation
    private void fr3() throws SQLException {

        System.out.println("FR3: Reservation Change\r\n");
        
		// Step 1: Establish connection to RDBMS
		try (Connection conn = DriverManager.getConnection(System.getenv("HP_JDBC_URL"),
							   System.getenv("HP_JDBC_USER"),
							   System.getenv("HP_JDBC_PW"))) {
	    // Step 2: Construct SQL statement
	    Scanner scanner = new Scanner(System.in);

        System.out.println("If you do not want to change a particular detail of the reservation, type 'no change'. Thank you.");
	    
        System.out.print("Enter a first name: ");
	    String firstName = scanner.nextLine();

	    System.out.format("Enter a last name: ");
        String lastName = scanner.nextLine();

        System.out.format("Enter the room code of the reservation: ");
        String roomCode = scanner.nextLine();

        System.out.format("Enter a begin date (YYYY-MM-DD): ");
        String beginDate = scanner.nextLine();

        System.out.format("Enter an end date (YYYY-MM-DD): ");
        String endDate = scanner.nextLine();

        System.out.format("Enter number of children: ");
        String numChildren = scanner.nextLine();

        System.out.format("Enter number of adults: ");
        String numAdults = scanner.nextLine();

        List<Object> params = new ArrayList<Object>();

        StringBuilder sb = new StringBuilder( """
            UPDATE lab7_reservations
            SET
                """
            );

            boolean changeStartDate = false;
            boolean changeEndDate = false;


            if (!"no change".equalsIgnoreCase(firstName)) {
                sb.append(" FirstName = ?, ");
                params.add(firstName);
            }
    
            if (!"no change".equalsIgnoreCase(lastName)) {
                sb.append(" LastName = ?,");
                params.add(lastName);
            }

            if (!"no change".equalsIgnoreCase(beginDate)) {
                sb.append(" CheckIn = ?,");
                changeStartDate = true;
                params.add(beginDate);
            }
    
            if (!"no change".equalsIgnoreCase(endDate)) {
                sb.append(" CheckOut = ?,");
                changeEndDate = true;
                params.add(endDate);
            }

            if (!"no change".equalsIgnoreCase(numAdults)) {
                sb.append(" Adults = ?,");
                params.add(numAdults);
            }
    
            if (!"no change".equalsIgnoreCase(numChildren)) {
                sb.append(" Kids = ?,");
                params.add(numChildren);
            }

            // 
            sb.setLength(sb.length() - 1);

            // System.out.println(sb.toString());

            sb.append(" WHERE CODE = ? ");
            params.add(roomCode);

            // System.out.println(sb.toString());

            if ( (changeEndDate && changeStartDate) == true) {

                params.add(beginDate);
                params.add(beginDate);
                params.add(endDate);
                params.add(roomCode);
                params.add(endDate);
                params.add(beginDate);
                params.add(endDate);
                params.add(roomCode);


                // add both checks
                sb.append("""
                    AND ? NOT IN (
                        SELECT * FROM
                        (
                            SELECT Checkin as notValidCheckIn
                            FROM lab7_reservations as r1
                            where (r1.Checkin between ? and ?) and CODE != ?
                        ) invalidCheckIns
                    )
                    AND ? NOT IN (
                        SELECT * FROM
                        (
                            SELECT Checkout as notValidCheckout
                            FROM lab7_reservations as r1
                            where (r1.Checkout between ? and ?) and CODE != ?
                        ) invalidCheckOuts
                    )
                        """);
            }
            else if (changeStartDate == true) {

                params.add(beginDate);
                params.add(beginDate);

                String sql = "select Checkout from lab7_reservations where lab7_reservations.CODE = '" + roomCode + "'";

                 // Step 4: Send SQL statement to DBMS
	        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {

               // Step 5: Receive results
               while (rs.next()) {
                   String actualCheckout = rs.getString("Checkout");

                   // System.out.format("%s %n", actualCheckout);
                   params.add(actualCheckout);
               }
           }
                params.add(roomCode);
                // only checkin date was changed
                sb.append("""
                    AND ? NOT IN (
                        SELECT * FROM
                        (
                            SELECT Checkin as notValidCheckIn
                            FROM lab7_reservations as r1
                            where (r1.Checkin between ? and ?) and CODE != ?
                        ) invalidCheckIns
                    )
                        """);

            }
            else if (changeEndDate == true) {

                params.add(endDate);

                String sql = "select Checkin from lab7_reservations where lab7_reservations.CODE = '" + roomCode + "'";

                // Step 4: Send SQL statement to DBMS
           try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {

              // Step 5: Receive results
              while (rs.next()) {
                  String actualCheckin = rs.getString("Checkin");

                  // System.out.format("%s %n", actualCheckin);
                  params.add(actualCheckin);
              }
          }
                params.add(endDate);
                params.add(roomCode);

                // only checkout date was changed
                sb.append("""
                    AND ? NOT IN (
                        SELECT * FROM
                        (
                            SELECT Checkout as notValidCheckout
                            FROM lab7_reservations as r1
                            where (r1.Checkout between ? and ?) and CODE != ?
                        ) invalidCheckOuts
                    )
                        """);

            }
            else if ( (changeEndDate && changeStartDate) == false) {
                // start and end dates were not changed
            }

             // Step 3: Start transaction
	        conn.setAutoCommit(false);

            try (PreparedStatement pstmt = conn.prepareStatement(sb.toString())) {
                int i = 1;
                for (Object p : params) {
                    pstmt.setObject(i++, p);
                }

                int rowCount = pstmt.executeUpdate();
		
		    // Step 5: Handle results
		    System.out.format("Updated %d reservation. Thank you.%n", rowCount);
            
            // Step 6: Commit or rollback transaction
            conn.commit();
            } catch (SQLException e) {
            conn.rollback();
            }
	    
    }

}

private void fr4() throws SQLException {
    System.out.println("FR4: Reservation Cancellation.\r\n");
    
    // Step 1: Establish connection to RDBMS
    try (Connection conn = DriverManager.getConnection(System.getenv("HP_JDBC_URL"),
                           System.getenv("HP_JDBC_USER"),
                           System.getenv("HP_JDBC_PW"))) {


            Scanner scanner = new Scanner(System.in);
            System.out.print("To cancel a reservation enter the reservation code: ");
            String roomCode = scanner.nextLine();

            System.out.print("Are you sure you want to cancel this reservation (yes/no)?: ");
            String confirmResponse = scanner.nextLine();

            // check for cancel
            if( (confirmResponse.equals("no")) || (confirmResponse.equals("No")) ) {
                // System.out.println("enter statement.");
                System.out.println("Reservation cancellation aborted.");
                System.exit(0);
            }
    
            String sql = "delete from lab7_reservations where lab7_reservations.CODE = ?";

            // Step 3: Start transaction
	    conn.setAutoCommit(false);
	    
	    try (PreparedStatement pstmtInsert = conn.prepareStatement(sql)) {
	
            // Step 4: Send SQL statement to DBMS
            pstmtInsert.setString(1, roomCode);
            
            int rowCount = pstmtInsert.executeUpdate();
		
            // Step 5: Handle results
            System.out.format("Deleted %d reservation. Reservation Cancelled.%n", rowCount);

		// Step 6: Commit or rollback transaction
		conn.commit();
	    } catch (SQLException e) {
		conn.rollback();
	    }
                           
                        
        }

}

private void fr5() throws SQLException {
    System.out.println("FR5: Search For Reservations.\r\n");

    // Step 1: Establish connection to RDBMS
    try (Connection conn = DriverManager.getConnection(System.getenv("HP_JDBC_URL"),
            System.getenv("HP_JDBC_USER"),
            System.getenv("HP_JDBC_PW"))) {
        // Step 2: Construct SQL statement
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter a search value for first name or 'Any': ");
        String firstName = scanner.nextLine();

        System.out.print("Enter a search value for last name or 'Any': ");
        String lastName = scanner.nextLine();

        System.out.print("Enter a search value for reservation code or 'Any': ");
        String resvCode = scanner.nextLine();

        System.out.print("Enter a search value for room code or 'Any': ");
        String roomCode = scanner.nextLine();

        System.out.format("Enter a begin date of stay in format (YYYY-MM-DD) or 'Any': ");
        String beginDate = scanner.nextLine();
        // LocalDate beginDate = LocalDate.parse(scanner.nextLine());
        System.out.format("Enter a end date of stay in format (YYYY-MM-DD) or 'Any': ");
        String endDate = scanner.nextLine();

        List<String> params = new ArrayList<String>();
        StringBuilder sb = new StringBuilder(
                """
                        SELECT CODE, Room, RoomName, CheckIn, Checkout, Rate, LastName, FirstName, Adults, Kids, decor
                        FROM lab7_reservations join lab7_rooms on
                        lab7_reservations.Room = lab7_rooms.RoomCode
                        WHERE
                                        """);

        // CHECK IF USER ENTERED THESE PARAMETERS
        if (!"Any".equalsIgnoreCase(firstName)) {
            sb.append(" FirstName like ? and ");
            params.add(firstName);
        }
        if (!"Any".equalsIgnoreCase(lastName)) {
            sb.append(" LastName like ? and ");
            params.add(lastName);
        }
        if (!"Any".equalsIgnoreCase(resvCode)) {
            sb.append(" CODE like ? and ");
            params.add(resvCode);
        }
        if (!"Any".equalsIgnoreCase(roomCode)) {
            sb.append(" Room like ? and ");
            params.add(roomCode);
        }
        if ((!"Any".equalsIgnoreCase(beginDate))) {
            sb.append(" CheckIn <= ? and ");
            params.add(beginDate);
        }
        if (!"Any".equalsIgnoreCase(endDate)) {
            sb.append(" CheckOut >= ? and ");
            params.add(endDate);
        }

        // REMOVE TRAILING AND
        sb.setLength(sb.length() - 4);

        // Step 3: Start transaction
        conn.setAutoCommit(false);

        try (PreparedStatement pstmt = conn.prepareStatement(sb.toString())) {

            int i = 1;
            for (String p : params) {
                pstmt.setObject(i++, p + "%");
            }

            // System.out.println(pstmt);

            try (ResultSet rs = pstmt.executeQuery()) {
                System.out.println("Matching reservations:");
                int matchCount = 0;
                while (rs.next()) {
                    // SELECT CODE, Room, RoomName, CheckIn, Checkout, Rate, LastName, FirstName,
                    // Adults, Kids, decor
                    System.out.format("%s %s %s %s %s ($%.2f) %s %s %s %s %s %n", rs.getString("CODE"),
                            rs.getString("Room"),
                            rs.getString("RoomName"),
                            rs.getString("CheckIn"),
                            rs.getString("Checkout"),
                            rs.getDouble("Rate"),
                            rs.getString("LastName"),
                            rs.getString("FirstName"),
                            rs.getString("Adults"),
                            rs.getString("Kids"),
                            rs.getString("decor"));
                    matchCount++;
                }
                System.out.format("----------------------%nFound %d match%s %n", matchCount,
                        matchCount == 1 ? "" : "es");
            }

        }

    }
                           
                    
}

private void fr6() throws SQLException {
    System.out.println("FR6: Revenue For Each Room.\r\n");
    
    // Step 1: Establish connection to RDBMS
    try (Connection conn = DriverManager.getConnection(System.getenv("HP_JDBC_URL"),
                           System.getenv("HP_JDBC_USER"),
                           System.getenv("HP_JDBC_PW"))) {

            
        String sql = """
            select ifnull(lab7_reservations.Room, "MonthRevenues") as "Rooms", 
            ifnull(round(SUM(CASE MONTH(Checkout) WHEN  1 THEN Rate END), 0), 0) AS January,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN  2 THEN Rate END), 0), 0) AS February,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN  3 THEN Rate END), 0), 0) AS March,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN  4 THEN Rate END), 0), 0) AS April,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN  5 THEN Rate END), 0), 0) AS May,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN  6 THEN Rate END), 0), 0) AS June,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN  7 THEN Rate END), 0), 0) AS July,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN  8 THEN Rate END), 0), 0) AS August,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN  9 THEN Rate END), 0), 0) AS September,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN 10 THEN Rate END), 0), 0) AS October,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN 11 THEN Rate END), 0), 0) AS November,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN 12 THEN Rate END), 0), 0) AS December,
              ifnull(round(SUM(CASE MONTH(Checkout) WHEN  1 THEN Rate END), 0), 0) 
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN  2 THEN Rate END), 0), 0) 
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN  3 THEN Rate END), 0), 0) 
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN  4 THEN Rate END), 0), 0) 
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN  5 THEN Rate END), 0), 0) 
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN  6 THEN Rate END), 0), 0) 
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN  7 THEN Rate END), 0), 0) 
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN  8 THEN Rate END), 0), 0)
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN  9 THEN Rate END), 0), 0) 
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN  10 THEN Rate END), 0), 0)
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN 11 THEN Rate END), 0), 0) 
              + ifnull(round(SUM(CASE MONTH(Checkout) WHEN  12 THEN Rate END), 0), 0) as "YearlyRevenues" 
    from lab7_reservations
    where year(curdate()) = year(checkout)
    group by lab7_reservations.Room with rollup
                """;

        
        // System.out.println("Rooms January February March April May June July August September October November December YearlyRevenues");
        // Step 4: Send SQL statement to DBMS
	    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {

            // Step 5: Receive results
            while (rs.next()) {
                
                String roomName = rs.getString("Rooms");
                String jan = rs.getString("January");
                String feb = rs.getString("February");
                String march = rs.getString("March");
                String april = rs.getString("April");
                String may = rs.getString("May");
                String june = rs.getString("June");
                String july = rs.getString("July");
                String aug = rs.getString("August");
                String sept = rs.getString("September");
                String oct = rs.getString("October");
                String nov = rs.getString("November");
                String dec = rs.getString("December");
                String yearlyTotal = rs.getString("YearlyRevenues");

                System.out.format("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %n", roomName, jan, feb, march, april,
                may, june, july, aug, sept, oct, nov, dec, yearlyTotal);
            }
       }
                                    
                        
             }
                           
                    
}
    
    // entire class 
}
