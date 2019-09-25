#' Create a new PostGIS database to hold data derived from LIDAR images
#'
#' This function first checks that PostgreSQL version 11 and the PostGIS
#' extension are installed on the local system and available from the command
#' line. It then creates a new database containing the PostGIS extension and
#' the required schemas and tables for data derived from LIDAR images.
#'
#' Three schemas are created in the new database: 'postgis' (to hold tables and
#' views used by the PostGIS extension); 'rasters' (to hold raster data tables);
#' 'vectors' (to hold vector data tables).
#'
#' Two raster data tables are created to store multi-band rasters of integer
#' point counts within voxels. Each table has two columns: \code{rid}: an
#' auto-incremented integer record number; \code{rast} raster tiles (i.e.
#' PostGIS storage tiles rather than LAS images). The first table,
#' 'rasters.point_counts', holds rasters derived from individual LAS images. The
#' PostGIS tile size for each record corresponds to the dimensions of the input
#' poinit counts raster. The second table, 'rasters.point_counts_union', holds
#' rasters that have been mosaiced by summing the values of overlapping edge
#' pixels. The reason for holding duplicate data is to allow for deletion and
#' re-importing of data (e.g. to fix artefacts in a particular raster) that
#' would otherwise be difficult with just the mosaiced data.
#'
#' A vector data table, 'vectors.las_metadata', is created to store metadata LAS
#' images including source file name, capture date and times, point counts and
#' average point density. The table also has a geometry column for the bounding
#' rectangle of each LAS image.
#'
#' There is no field explicitly linking raster records to records in other
#' tables (e.g. the LAS metadata table). Such relationships are found via
#' spatial queries.
#'
#' @param dbname Database name.
#'
#' @param host Host name. Defaults to \code{'localhost'}.
#'
#' @param port Port number to use for database commands. Defaults to \code{5432}
#'   which is the default port number for PostgreSQL.
#'
#' @param username User name for this session. Defaults to \code{'postgres'}.
#'
#' @param password Database password for the specified user. If \code{NULL}
#'   (default) this is taken from the local system environment variable
#'   \code{PGPASSWORD}.
#'
#' @param pgpath The path to the \code{bin} directory of a locally accessible
#'   installation of PostgreSQL / PostGIS. Defaults to
#'   \code{'c:/Program Files/PostgreSQL/11/bin'}.
#'
#' @return A named list of database settings for use with other \code{db_xxx}
#'   functions.
#'
#' @seealso \code{\link{db_connect_postgis}}
#'
#' @export
#'
db_create_postgis <- function(dbname,
                              host = "localhost",
                              port = 5432,
                              username = "postgres",
                              password = NULL,
                              pgpath = "c:/Program Files/PostgreSQL/11/bin") {

  helper.list <- .check_database_installation(pgpath)
  password <- .check_database_password(password)

  # First connect to PostgreSQL default database
  p <- pool::dbPool(
    drv = RPostgreSQL::PostgreSQL(),
    dbname = "",
    host = host,
    port = port,
    user = username,
    password = password)

  # Create database and install PostGIS
  pool::dbExecute(p, glue::glue("CREATE DATABASE {dbname};") )

  # Switch connection to the new database
  pool::poolClose(p)

  p <- pool::dbPool(
    drv = RPostgreSQL::PostgreSQL(),
    dbname = dbname,
    host = host,
    port = port,
    user = username,
    password = password)

  # Install PostGIS
  pool::dbExecute(p, "CREATE SCHEMA postgis;")
  pool::dbExecute(p, "CREATE EXTENSION postgis WITH SCHEMA postgis;")

  # Create required schemas and tables
  pool::dbExecute(p, "CREATE SCHEMA rasters;")
  pool::dbExecute(p, "CREATE SCHEMA vectors;")

  pool::dbExecute(p, glue::glue("ALTER DATABASE {dbname}
                                SET search_path TO rasters, vectors, postgis, public;"))

  # Disconnect and reconnect to ensure search path and PostGIS support
  pool::poolClose(p)

  p <- pool::dbPool(
    drv = RPostgreSQL::PostgreSQL(),
    dbname = dbname,
    host = host,
    port = port,
    user = username,
    password = password)

  # Create point count tables
  tblnames <- .table_names()

  command <- glue::glue("CREATE TABLE IF NOT EXISTS {tblnames$TABLE_COUNTS_RAW} \\
                        (rid serial primary key, rast raster);")

  pool::dbExecute(p, command)

  command <- glue::glue("CREATE TABLE IF NOT EXISTS {tblnames$TABLE_COUNTS_UNION} \\
                        (rid serial primary key, rast raster);")

  pool::dbExecute(p, command)

  # Create LAS metadata table
  command <- glue::glue(
    "CREATE TABLE IF NOT EXISTS \\
    {tblnames$TABLE_METADATA} (\\
    id serial PRIMARY KEY, \\
    filename text NOT NULL, \\
    capture_start timestamp with time zone NOT NULL, \\
    capture_end timestamp with time zone NOT NULL, \\
    area double precision NOT NULL, \\
    point_density double precision NOT NULL, \\
    npts_ground integer NOT NULL, \\
    npts_veg integer NOT NULL, \\
    npts_building integer NOT NULL, \\
    npts_water integer NOT NULL, \\
    npts_other integer NOT NULL, \\
    npts_total integer NOT NULL, \\
    nflightlines integer NOT NULL, \\
    bounds geometry(Polygon, 3308) NOT NULL);" )

  pool::dbExecute(p, command)


  # Return settings list
  c(
    helper.list,
    tblnames,
    list(
      'DBNAME' = dbname,
      'USERNAME' = username,
      'PASSWORD' = password,
      'POOL' = p)
  )
}


#' Connect to a PostGIS database holding data derived from LIDAR images
#'
#' This function first checks that PostgreSQL version 11 and the PostGIS
#' extension are installed on the local system and available from the command
#' line. It then creates a database connection pool to provide for simple
#' queries from R and returns this and other database parameters in a named list
#' to be used with \code{db_xxx} functions.
#'
#' @param dbname Database name.
#'
#' @param host Host name. Defaults to \code{'localhost'}.
#'
#' @param port Port number to use for database commands. Defaults to \code{5432}
#'   which is the default port number for PostgreSQL.
#'
#' @param username User name for this session. Defaults to \code{'postgres'}.
#'
#' @param password Database password for the specified user. If \code{NULL}
#'   (default) this is taken from the local system environment variable
#'   \code{PGPASSWORD}.
#'
#' @param pgpath The path to the \code{bin} directory of a locally accessible
#'   installation of PostgreSQL / PostGIS. Defaults to
#'   \code{'c:/Program Files/PostgreSQL/11/bin'}.
#'
#' @return A named list of database settings for use with other \code{db_xxx}
#'   functions.
#'
#' @seealso \code{\link{db_create_postgis}}
#'
#' @export
#'
db_connect_postgis <- function(dbname,
                               host = "localhost",
                               port = 5432,
                               username = "postgres",
                               password = NULL,
                               pgpath = "c:/Program Files/PostgreSQL/11/bin") {

  helper.list <- .check_database_installation(pgpath)
  password <- .check_database_password(password)

  # Create database connection pool
  p <- pool::dbPool(
    drv = RPostgreSQL::PostgreSQL(),
    dbname = dbname,
    host = host,
    port = port,
    user = username,
    password = password)

  # Return settings list
  c(
    helper.list,
    .table_names(),
    list(
    'DBNAME' = dbname,
    'USERNAME' = username,
    'PASSWORD' = password,
    'POOL' = p)
  )
}


.table_names <- function() {
  list(
    'TABLE_COUNTS_RAW' = "rasters.point_counts",
    'TABLE_COUNTS_UNION' = "rasters.point_counts_union",
    'TABLE_METADATA' = "vectors.las_metadata"
  )
}


.check_database_installation <- function(pgpath) {
  if (!dir.exists(pgpath)) stop("Cannot find path ", pgpath)

  # Locate required helper applications
  exes <- dir(pgpath, pattern = "exe$", full.names = TRUE)
  helpers <- c("psql.exe", "raster2pgsql.exe")
  ii <- sapply(helpers, grep, exes)
  ok <- sapply(ii, length) > 0

  if (any(!ok))
    stop("Cannot find required database helper applications\n", helpers[!ok])

  list(
    'PSQL' = exes[ ii["psql.exe"] ],
    'R2P' = exes[ ii["raster2pgsql.exe"] ]
  )
}


.check_database_password <- function(password) {
  if (is.null(password)) {
    password <- Sys.getenv("PGPASSWORD")
    if (password == "") {
      stop("Database password not set. Provide it as an argument\n",
           "or set the environment variable PGPASSWORD")
    }
  }

  password
}


