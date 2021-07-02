package org.functionstream.functions.token;

import java.sql.*;

public class TokenJDBC {

    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf8&useSSL=false&serverTimezone=CST";
    private static final String username = "root";
    private static final String password = "root";

    public static boolean checkAccount(String userIn, String passwordIn){
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        boolean flag=false;
        try {
            Class.forName(DRIVER_CLASS);
            connection = DriverManager.getConnection(url, username, password);
            String sql = "select * from account where username = ? and password = ?";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,userIn);
            preparedStatement.setString(2,passwordIn);
            resultSet = preparedStatement.executeQuery();
            flag = resultSet.next();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            if(null != resultSet){
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(null != preparedStatement){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(null != connection){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return flag;
    }
}
