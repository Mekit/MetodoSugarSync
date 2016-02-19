Metodo SugarCRM Sync
=====================

USAGE - CRON
-----------------------------
./console sync:cron cron.yml


USAGE(METODO->CRM) - ACCOUNTS
-----------------------------
./console sync-down:accounts accounts_down.yml --update-cache

./console sync-down:accounts accounts_down.yml --update-remote


USAGE(METODO->CRM) - CONTACTS
-----------------------------
./console sync-down:contacts contacts_down.yml --update-cache

./console sync-down:contacts contacts_down.yml --update-remote


USAGE(METODO->CRM) - RELATIONSHIP ACCOUNTS - CONTACTS
------------------------------------------------------
./console sync-down:rel-acc-cnt rel_acc_cnt_down.yml --update-cache

./console sync-down:rel-acc-cnt rel_acc_cnt_down.yml --update-remote


USAGE(CRM->METODO) - ACCOUNTS
-----------------------------
./console sync-up:accounts accounts_up.yml


USAGE(CRM->METODO) - CONTACTS
-----------------------------
./console sync-up:contacts contacts_up.yml




1) Install the packages
---------------------
    apt-get install freetds-bin freetds-common tdsodbc odbcinst php5-odbc unixodbc
    
2) Restart your webserver to load the ODBC module into PHP
---------------------------------------------------------
    service apache2 (reload|restart)
    
3) Get Info about MSSql server
-------------------------------
    tsql -LH SQL_SERVER_IP
    
this will give something like:

    ServerName SERVER_NAME
    InstanceName SERVER_INSTANCE
    IsClustered No
    Version 11.0.2100.60
    tcp SQL_SERVER_PORT
      
4) Try direct connection with tsql
--------------------------------
    TDSVER=8.0 tsql -H SQL_SERVER_IP -p SQL_SERVER_PORT -U "USERNAME" -P "PASSWORD"
    
If no cookies, try to change TDSVER (5.0, 7.0, 7.1, 8.0)
If you get command prompt, use "quit" to exit
    


5) Set up /etc/freetds/freetds.conf
---------------------------------
    [SERVER_NAME]
    host = SQL_SERVER_IP
    port = SQL_SERVER_PORT
    tds version = TDSVER
    
    
6) Try connection through server name with tsql
-----------------------------------------------
    tsql -S SERVER_NAME -U "USERNAME" -P "PASSWORD"
  
7) Setup ODBC Driver (/etc/odbcinst.ini)
------------------------------------------------
    [freetds]
    Description = MS SQL database access with Free TDS
    #Driver     = /usr/lib/i386-linux-gnu/odbc/libtdsodbc.so
    Driver      = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
    Setup       = /usr/lib/i386-linux-gnu/odbc/libtdsS.so
    UsageCount  = 1
  
The section name will be used in following step as the reference for the Driver. 
Make sure to use the correct driver for your platform.
 
8) Setup ODBC Connection (/etc/odbc.ini)
-----------------------------------------
For each database (or as required) set up a section with:

    [CONNECTION-DATABASE]
    Description             = Some description to identify this section
    Driver                  = freetds
    Database                = DATABASE_NAME
    ServerName              = SERVER_NAME
    TDS_Version             = TDSVER
    
  - The name of the section `[CONNECTION-DATABASE]` will be used in the DSN
  - The Description is a free text and can be omitted
  - Driver must correspond to one of the driver sections in /etc/odbcinst.ini - normally 'freetds'
  - Database is the name of the database that will be used in this connection
  - ServerName must correspond to one of the sections in /etc/freetds/freetds.conf
  - TDS_Version must match the version defined in the section you are using from /etc/freetds/freetds.conf



9) Try to connect from php
-----------------------------------------
    try {
      $db = new PDO("odbc:$serverName", "$username", "$password");
    } catch(PDOException $exception) {
      die("Unable to open database!\n" . $exception->getMessage() . "\n");
    }
    echo "Successfully connected!\n";
    
