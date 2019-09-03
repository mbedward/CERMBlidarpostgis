#' Checks for a local PostgreSQL/PostGIS installation and sets runtime variables
#'
#' This function first checks that PostgreSQL version 11 and the PostGIS
#' extension are installed on the local system and available from the command
#' line. It then creates a database connection pool to provide for simple
#' queries from R.
#'
#' @param quiet If \code{TRUE}, warning messages about missing components are
#'   not printed to the console. The default is \code{FALSE}.
#'
#' @return \code{TRUE} if PostgreSQL/PostGIS is found;
#'   \code{FALSE} otherwise.
#'
#' @export
#'
connect_to_database <- function(dbname,
                                host = "localhost",
                                port = 5432,
                                username = "postgres",
                                password = NULL,
                                pgpath = "c:/Program Files/PostgreSQL/11/bin") {

  if (!dir.exists(pgpath)) {
    if (!quiet)
      warning("Cannot find path ", pgpath)
    return(NULL)
  }

  # Locate required helper applications
  exes <- dir(pgpath, pattern = "exe$", full.names = TRUE)
  helpers <- c("psql.exe", "raster2pgsql.exe")
  ii <- sapply(helpers, grep, exes)
  ok <- sapply(ii, length) > 0

  if (any(!ok)) {
    warning("Cannot find required database helper applications\n", helpers[!ok])
    return(NULL)
  }

  if (is.null(password)) {
    password <- Sys.getenv("PGPASSWORD")
    if (password == "") {
      warning("Database password not set. Provide it as an argument\n",
              "or set the environment variable PGPASSWORD")
      return(NULL)
    }
  }

  # Create database connection pool
  pool <- pool::dbPool(
    drv = RPostgreSQL::PostgreSQL(),
    dbname = dbname,
    host = host,
    port = port,
    user = username,
    password = password)

  # Return settings
  list(
    'DBNAME' = dbname,
    'USERNAME' = username,
    'PSQL' = exes[ ii["psql.exe"] ],
    'R2P' = exes[ ii["raster2pgsql.exe"] ],
    'POOL' = pool)
}


# Private getter functions

.settings_get_dbname <- function(dbsettings) {
  .get_dbsetting(dbsettings, 'DBNAME')
}

.settings_get_username <- function(dbsettings) {
  .get_dbsetting(dbsettings, 'USERNAME')
}

.settings_get_pool <- function(dbsettings) {
  .get_dbsetting(dbsettings, 'POOL')
}

.settings_get_psql <- function(dbsettings) {
  .get_dbsetting(dbsettings, 'PSQL')
}

.settings_get_r2p <- function(dbsettings) {
  .get_dbsetting(dbsettings, 'R2P')
}

.get_dbsetting <- function(dbsettings, setting.name) {
  val <- dbsettings[[setting.name]]
  if (is.null(val))
    stop("Database settings not ready. \nPlease see function connect_to_database.")

  val
}

