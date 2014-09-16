/* Simple C program that connects to MySQL Database server*/
#include <mysql.h>
#include <stdio.h>
main() {
  MYSQL *conn = mysql_init(NULL);
  
  if (conn == NULL) {
    fprintf(stderr, "%s\n", mysql_error(conn));
    exit(1);
  }

  if (mysql_real_connect(conn, "localhost", "fudd", "wabbit-season",
        NULL, 0, NULL, 0) == NULL) {
    fprintf(stderr, "%s\n", mysql_error(conn));
    mysql_close(conn);
    exit(1);
  }
}
