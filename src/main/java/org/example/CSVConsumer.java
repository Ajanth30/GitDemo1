package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
/* Changes in feature 1
*/

public class CSVConsumer {
    private static final String INSERT_INTO="insert into student_reg"+"(Student_Id,Student_Name,Gender,Adress,Contact)values"+"(?,?,?,?,?);";

    public static void main(String[] args) throws IOException {
        String jdbcUrl="jdbc:mysql://localhost:3306/database1";
        String uname="root";
        String password="Ajanth@330";
        String filePath="C:\\Users\\HP\\Desktop\\CSV to DB\\Book1.csv";
        int batchSize=20;

        try(Connection connection=DriverManager.getConnection(jdbcUrl,uname,password);
            PreparedStatement statement=connection.prepareStatement(INSERT_INTO);){
            connection.setAutoCommit(false);
            BufferedReader reader=new BufferedReader(new FileReader(filePath));
            int count=0;
            String readLine=null;
            reader.readLine();
            while ((readLine=reader.readLine())!=null){
                String [] data=readLine.split(",");
                String id=data[0];
                String name=data[1];
                String gender=data[2];
                String adress=data[3];
                String contact=data[4];
                statement.setString(1,id);
                statement.setString(2,name);
                statement.setString(3,gender);
                statement.setString(4,adress);
                statement.setString(5,contact);
                if(count%batchSize==0){
                    statement.executeUpdate();
                }


            }
            reader.close();
            connection.commit();
        }catch (SQLException e){
            System.out.println(e);
        }

    }
}
