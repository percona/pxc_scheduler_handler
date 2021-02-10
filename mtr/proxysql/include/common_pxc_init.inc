# Setup monitor user
--disable_query_log
CREATE USER 'monitor'@'%' IDENTIFIED WITH 'mysql_native_password' BY 'monitor';
GRANT ALL ON *.* TO 'monitor'@'%';

# Setup regular user
CREATE USER 'app_test'@'%' IDENTIFIED WITH 'mysql_native_password' BY 'test';
GRANT CREATE, DROP, DELETE, INSERT, SELECT, UPDATE ON *.* TO 'app_test'@'%';
#GRANT ALL ON *.* TO 'app_test'@'%';
--enable_query_log
