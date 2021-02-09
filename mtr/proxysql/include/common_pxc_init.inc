# Setup monitor user
CREATE USER 'monitor'@'%' IDENTIFIED BY 'monitor';
GRANT ALL ON *.* TO 'monitor'@'%';

# Setup regular user
CREATE USER 'app_test'@'%' IDENTIFIED WITH 'mysql_native_password' BY 'test';
GRANT ALL ON *.* TO 'app_test'@'%';
