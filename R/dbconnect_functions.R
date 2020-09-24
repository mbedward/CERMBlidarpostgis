#' Create a new PostGIS database to hold data derived from LIDAR images
#'
#' This function first checks that PostgreSQL version 11 and the PostGIS
#' extension are installed on the local system and available from the command
#' line. It then creates a new database containing the PostGIS extension and
#' the required schemas and tables for data derived from LIDAR images. Note that the PostGIS extension is presently
#' stored in the 'public' schema rather than a separate schema to allow users to
#' connect with ArcMap versions prior to 10.7.1.
#'
#' Schemas are created for each of the MGA zones to be supported (default: zones
#' 54, 55 and 56). Within each schema, two raster data tables are created to
#' store multi-band rasters of integer point counts within voxels. Each table
#' has two columns: \code{rid}: an auto-incremented integer record number;
#' \code{rast} raster tiles (i.e. PostGIS storage tiles rather than LAS images).
#' The first table, 'point_counts', holds rasters derived from individual LAS
#' images. The PostGIS tile size for each record corresponds to the dimensions
#' of the input poinit counts raster. The second table, 'point_counts_union',
#' holds rasters that have been mosaiced by summing the values of overlapping
#' edge pixels. The reason for holding duplicate data is to allow for deletion
#' and re-import of data (e.g. to fix problems in a particular LAS tile) that
#' would otherwise be difficult with just the mosaiced data.
#'
#' A vector data table, 'las_metadata', is created in each schema to store
#' metadata LAS images including source file name, capture date and times, point
#' counts and average point density. The table also has a geometry column for
#' the bounding rectangle of each LAS image.
#'
#' Records in the 'las_metadata' and 'point_counts' tables are related by a
#' common integer identifier (las_metadata.id = point_counts.metadata_id). No
#' field explicitly links raster records in the point_counts_union table to
#' those in other tables. Instead, records can be related via spatial queries.
#'
#' @param dbname Database name.
#'
#' @param mga.zones MGA map zones (two digit integer) for which schema should
#'   be created. Presently, only GDA94 map zones 49-56 are supported (EPSG 28349
#'   - 28356). The default is \code{54:56} for New South Wales map zones.
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
                              mga.zones = c(54, 55, 56),
                              host = "localhost",
                              port = 5432,
                              username = "postgres",
                              password = NULL,
                              pgpath = "c:/Program Files/PostgreSQL/11/bin") {

  stopifnot(is.character(dbname) && length(dbname) == 1)
  stopifnot(is.numeric(mga.zones) && length(mga.zones) >= 1)

  # Only MGA zones are supported at the moment
  stopifnot(all(mga.zones %in% 49:56))

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
  # ArcGIS prior to v10.7.1 does not work when PostGIS is installed
  # somewhere other than the public schema
  #pool::dbExecute(p, "CREATE SCHEMA postgis;")
  #pool::dbExecute(p, "CREATE EXTENSION postgis WITH SCHEMA postgis;")
  pool::dbExecute(p, "CREATE EXTENSION postgis;")

  tblnames <- .table_names()

  schemas <- data.frame(
    schema = paste0("mgazone", mga.zones),
    epsg = 28300 + mga.zones,
    stringsAsFactors = FALSE
  )

  for (ischema in 1:nrow(schemas)) {
    schema <- schemas[ischema, "schema"]
    epsg <- schemas[ischema, "epsg"]

    # Create required schemas and tables
    pool::dbExecute(p, glue::glue("CREATE SCHEMA {schema};"))

    # Table for LAS tile metadata
    command <- glue::glue(
      "CREATE TABLE IF NOT EXISTS \\
      {schema}.{tblnames$TABLE_METADATA} (
      id serial PRIMARY KEY,
      filename text NOT NULL,
      purpose text NOT NULL CHECK (purpose IN ('general', 'postfire')),
      capture_start timestamp with time zone NOT NULL,
      capture_end timestamp with time zone NOT NULL,
      area double precision NOT NULL,
      point_density double precision NOT NULL,
      npts_ground integer NOT NULL,
      npts_veg integer NOT NULL,
      npts_building integer NOT NULL,
      npts_water integer NOT NULL,
      npts_other integer NOT NULL,
      npts_total integer NOT NULL,
      nflightlines integer NOT NULL,
      bounds geometry(Polygon, {epsg}) NOT NULL,
      mapname text NOT NULL,
      );" )

    pool::dbExecute(p, command)

    # Table for building points
    command <- glue::glue(
      "CREATE TABLE IF NOT EXISTS \\
      {schema}.{tblnames$TABLE_BUILDINGS} (
      id serial PRIMARY KEY,
      meta_id integer references {schema}.las_metadata(id),
      height double precision NOT NULL,
      geom geometry(Point, {epsg}) NOT NULL);"
    )

    pool::dbExecute(p, command)

    # Table for unmerged LAS tile point counts
    command <- glue::glue("CREATE TABLE IF NOT EXISTS \\
                        {schema}.{tblnames$TABLE_COUNTS_LAS}
                        (rid serial primary key,
                         meta_id integer references {schema}.las_metadata(id),
                         rast raster);")

    pool::dbExecute(p, command)

    # Table for merged point counts
    command <- glue::glue("CREATE TABLE IF NOT EXISTS \\
                        {schema}.{tblnames$TABLE_COUNTS_UNION}
                        (rid serial primary key, rast raster);")

    pool::dbExecute(p, command)
  }


  # Set search path
  schema.path <- paste(schemas[["schema"]], collapse = ", ")

  pool::dbExecute(p, glue::glue("ALTER DATABASE {dbname} \\
                                SET search_path TO \\
                                {schema.path}, public;"))

  # Disconnect and reconnect to ensure search path and PostGIS support
  pool::poolClose(p)

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
    schemas,
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
                               host = NULL,
                               port = 5432,
                               username = NULL,
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

  # Retrieve names of schemas and check that one or more of them correspond to supported
  # MGA map zones 49 - 56
  x <- pool::dbGetQuery(p, "SELECT schema_name AS name FROM information_schema.schemata;")
  x <- stringr::str_subset(x$name, "^mgazone\\d{2}")

  if (!any(x %in% paste0("mgazone", 49:56))) {
    stop("Did not find any database schemas for MGA map zones 49-56")
  }

  epsgs <- 28300 + as.integer(stringr::str_extract(x, "\\d{2}$"))

  schemas <- data.frame(
    schema = x,
    epsg = epsgs,
    stringsAsFactors = FALSE
  )

  # Return settings list
  c(
    helper.list,
    .table_names(),
    schemas,
    list(
    'DBNAME' = dbname,
    'USERNAME' = username,
    'PASSWORD' = password,
    'POOL' = p)
  )
}


#' Close an open connection to a PostGIS LiDAR database
#'
#' This is a convenience function, equivalent to calling
#' \code{pool::poolClose(DBsettings$POOL)} directly.
#'
#' @param DBsettings A named list of database settings as returned by
#'   \code{db_create_postgis} and \code{db_connect_postgis}.
#'
#' @export
#'
db_disconnect_postgis <- function(dbsettings) {
  pool::poolClose(dbsettings$POOL)
}


.table_names <- function() {
  list(
    'TABLE_COUNTS_LAS' = "point_counts",
    'TABLE_COUNTS_UNION' = "point_counts_union",
    'TABLE_METADATA' = "las_metadata",
    'TABLE_BUILDINGS' = "building_points"
  )
}


.get_pool <- function(dbsettings, on.fail = c("error", "warning", "silent")) {
  on.fail <- match.arg(on.fail)

  p <- dbsettings$POOL
  ok <- !is.null(p) && pool::dbIsValid(p)

  if (ok) {
    p
  } else {
    base::switch(on.fail,
                 silent = NULL,

                 warning = {
                   warning("Database connection is closed or invalid", call. = FALSE)
                   NULL
                 },

                 error = stop("Database connection is closed or invalid", call. = FALSE)
    )
  }
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


.check_database_password <- function(password = NULL) {
  if (is.null(password)) {
    # Check for pgpass.conf file
    if (tolower(Sys.info()["sysname"]) == "windows") {
      # On Windows, look for pgpass.conf and, if found,
      # ASSUME that the password is set there
      path <- file.path(Sys.getenv("APPDATA"), "postgresql", "pgpass.conf")
      if (file.exists(path)) return(NULL)
    } else {
      # TODO - code for *nix type systems
      stop("Bummer - password code still needs to be written for non-Windows OS")
    }

    # If we get here, we are on Windows and the pgpass.conf file
    # was not found
    password <- Sys.getenv("PGPASSWORD", unset = NA)

    if (is.na(password)) {
      stop("Database password not set. Provide it as an argument\n",
           "or set the environment variable PGPASSWORD")
    }
  }

  password
}


