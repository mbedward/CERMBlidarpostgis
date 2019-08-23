#' Run an SQL command on a selected database
#'
#' This function allows you to run arbitrary SQL commands on a selected database
#' via the active PostgreSQL server. It is an alternative to connecting to the
#' database via the RPostgreSQL package or similar. If the SQL commands are
#' provided as a character vector, they are run by first writing them to a
#' temporary file which is passed to the command line program \code{psql.exe}.
#' Multiple commands can be provided in concatenated form as either a
#' one-element character vector, or as a multi-element vector. In both cases,
#' each command must be followed by a semicolon as per normal SQL syntax (but
#' see Details for an exception to this rule in the case of special \code{psql}
#' commands).
#'
#' The \code{psql} command line program installed with PostgreSQL supports a
#' set of special commands in addition to standard SQL statements. For example,
#' \code{'\d mytable'} will return a description of the specified table while
#' \code{'\dt'} will list all tables in the database. These commands can be
#' issued via the \code{pg_sql} function and do not require a following
#' semi-colon. Note however that R will require the leading backslash to be
#' 'escaped', e.g. \code{'\\dt'} otherwise obscure error messages will result.
#'
#' @param command A valid SQL command as a character string or \code{glue}
#'   object. Can also be a special \code{psql} command (see Details). Not
#'   required if the \code{file} argument is used to read commands from a file.
#'
#' @param file The path and name of a file containing the SQL command(s) to
#'   run. Ignored if \code{command} is provided.
#'
#' @param dbname Name of an existing database to run the command against, or an
#'   empty string \code{""} for commands such as \code{'CREATE DATABASE foo;'}.
#'
#' @param username Name of the user (default: 'postgres').
#'
#' @param quiet If \code{TRUE} the output from the psql helper application is
#'   returned invisibly; if \code{FALSE} (default) the output is returned
#'   explicitly.
#'
#' @examples
#' \dontrun{
#' # Must do this once at the start of an R session
#' Sys.setenv(PGPASSWORD = "cermb")
#'
#' pg_sql("SELECT COUNT(*) AS NRECS FROM FOO;", dbname = "mydb")
#' }
#'
#' @export
#'
pg_sql <- function(command, file = NULL, dbname = "",
                   username = "postgres", quiet = TRUE) {

  PSQL <- .get_runtime_setting("PSQL")
  if (is.null(PSQL))
    stop("Runtime settings not ready. Have you called check_postgis()?")

  command <- .parse_command(command)
  if (!is.null(command)) {
    fsql <- tempfile(pattern = "sql", fileext = ".sql")
    cat(command, file = fsql)
  } else {
    if (!file.exists(file)) stop("No command provided so expected a file name")
    fsql <- file
  }

  dbarg <- ifelse(dbname == "", "", glue::glue('-d {dbname}'))
  args <- glue::glue('{dbarg} -U {username} -f {fsql}')
  out <- system2(command = PSQL, args = args, stdout = TRUE)

  unlink(fsql)

  if (quiet)
    invisible(out)
  else
    out
}


# Helper for pg_sql function.
# Returns NULL if command is empty or only white space.
# Flattens multi-element character vectors of commands to a single element.
#
.parse_command <- function(command) {
  command <- stringr::str_trim(command)
  if (is.na(command) || length(command) == 0)
    NULL
  else {
    if (length(command > 1)) {
      command <- stringr::str_subset(command, "[^\\s]")
      command <- stringr::str_trim( paste(command, collapse = " ") )
    }

    if (nchar(command) == 0) NULL
    else command
  }
}



#' Import a raster into the database
#'
#' @export
#'
pg_load_raster <- function(r, epsg, dbname, schema, tbl,
                           replace = FALSE, tilew = 50, username = "postgres") {

  PSQL <- .get_runtime_setting("PSQL")
  R2P <- .get_runtime_setting("R2P")
  if (is.null(PSQL) || is.null(R2P))
    stop("Runtime settings not ready. Have you called check_postgis()?")

  is.tbl <- pg_table_exists(dbname, schema, tbl, username)
  if (replace) {
    in.mode <- "-d"
  } else if (is.tbl) {
    in.mode <- "-a"
  } else {
    in.mode <- "-c"
  }

  ftif <- tempfile(pattern = "raster", fileext = ".tif")
  fsql <- tempfile(pattern = "sql", fileext = ".txt")

  raster::writeRaster(r, filename = ftif,
                      format = "GTiff",
                      overwrite = TRUE)


  args <- glue::glue('{in.mode} -s {epsg} -t {tilew}x{tilew} -M -Y {ftif} {schema}.{tbl}')
  system2(command = R2P, args = args, stdout = fsql)

  args <- glue::glue('-d {dbname} -U {username} -f {fsql}')
  system2(command = PSQL, args = args)

  unlink(ftif)
  unlink(fsql)
}


#' Check if a table exists in the given database
#'
#' The check is done by using the \code{psql.exe} helper program to retrieve
#' metadata for the table, and checking that something was found.
#'
#' @param dbname Name of an existing database within the PostgreSQL server.
#'
#' @param schema Name of the schema containing the table.
#'
#' @param tbl Name of the table.
#'
#' @param username Name of the user (default: 'postgres').
#'
#' @return \code{TRUE} if the table was found; \code{FALSE} otherwise.
#'
#' @examples
#' \dontrun{
#' pg_table_exists("nsw_lidar", "postgis", "bega")
#' }
#'
#' @export
#'
pg_table_exists <- function(dbname, schema, tbl, username = "postgres") {
  tblname <- glue::glue('{schema}.{tbl}')
  command <- glue::glue('\\d {tblname}')
  x <- pg_sql(command, dbname)

  ptn <- glue::glue('table.+{tblname}')
  stringr::str_detect(tolower(x[1]), ptn)
}

