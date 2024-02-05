/*
 *
 * Copyright (c) 2015. DENODO Technologies.
 * http://www.denodo.com
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of DENODO
 * Technologies ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with DENODO.
 */

package com.denodo.vdp.demo.jdbcclient;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;

/**
 * This sample uses the Denodo JDBC driver to connect to Virtual DataPort and query the views <tt>sales</tt> and
 * <tt>average_montly_sales</tt>.
 * 
 * @param args
 *            "url(like '//localhost:9999/admin')", "login", and "password"
 */
public class SalesClient {

    /**
     * Main class method.
     */
    public static void main(String args[]) {

        // Check parameters number (uri, user, password)
        if (args.length != 3) {

            System.out.println("Invalid parameters number");
            System.out.println("Usage: java com.denodo.vdp.demo.jdbcclient.SalesClient " + "<uri> <login> <password>");
            System.out.println("Example: java com.denodo.vdp.demo.jdbcclient.JDBCClient "
                    + "\"jdbc:vdb://localhost:9999/admin\" admin admin");
            System.exit(-1);
        }

        System.out.println("Connecting to database ... '" + args[0] + "'");
        System.out.println("User: '" + args[1] + "'");
        System.out.println("Password: '" + args[2] + "'");

        // JDBC Driver load
        try {

            Class.forName("com.denodo.vdp.jdbc.Driver");

        } catch (Exception e) {

            System.err.println("Error loading VDPDriver ... " + e.getMessage());
            System.exit(-1);
        }

        Connection connection = null;
        try {

/////////////////////////////////////////////////////////////////////////////
////////////////////////// FIRST QUERY //////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

            // Connect the driver to the database            
            connection = DriverManager.getConnection(args[0], args[1], args[2]);

            // Get the connection from the server and prepare the query
            System.out.println("Preparing first statement " + "'SELECT * FROM client'...  ");
            Statement statement = connection.createStatement();

            // Execute the query obtaining a VDBResultSetIterator
            System.out.println("Executing query...  ");
            ResultSet result = statement.executeQuery("select * from client");
            System.out.println("OK");

            // Obtaining the result of the query and printing it.
           // ResultSet result = statement.getResultSet();
            if (result != null) {
                ResultSetMetaData metadata = result.getMetaData();
                int max = metadata.getColumnCount();

                System.out.print("| ");
                for (int i = 1; i <= max; i++) {

                    System.out.print(metadata.getColumnName(i) + ":" + metadata.getColumnTypeName(i) + " | ");
                }
                System.out.println("\n");

                // Iterate over the rows returned
                while (result.next()) {

                    System.out.print("| ");
                    // Iterate over the columns of the row
                    for (int i = 1; i <= max; i++) {

                        System.out.print(result.getObject(i) + " | ");
                    }
                    System.out.println();
                }
            } else {

                System.err.println("Error in result");
            }

        } catch (Exception e) {

            System.err.println("Error accesing the DB ... ");
            e.printStackTrace();

        } finally {

            // The connection must be closed.
            if (connection != null) {

                try {

                    connection.close();

                } catch (SQLException e) {

                    System.err.println("Error closing connection ... ");
                    e.printStackTrace();
                }
            }
        }
    }
}