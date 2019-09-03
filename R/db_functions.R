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
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param command A valid SQL command as a character string or \code{glue}
#'   object. Can also be a special \code{psql} command (see Details). Not
#'   required if the \code{file} argument is used to read commands from a file.
#'
#' @param file The path and name of a file containing the SQL command(s) to
#'   run. Ignored if \code{command} is provided.
#'
#' @param quiet If \code{TRUE} the output from the psql helper application is
#'   returned invisibly; if \code{FALSE} (default) the output is returned
#'   explicitly.
#'
#' @return Text of the database status message after running the command.
#'
#' @examples
#' \dontrun{
#' # Must do this once at the start of an R session
#' Sys.setenv(PGPASSWORD = "cermb")
#'
#' dbsettings <- connect_to_database("cermb_lidar")
#'
#' pg_sql(dbsettings, "SELECT COUNT(*) AS NRECS FROM FOO;")
#' }
#'
#' @export
#'
pg_sql <- function(dbsettings, command = NULL, file = NULL, quiet = TRUE) {

  PSQL <- .settings_get_psql(dbsettings)
  dbname <- .settings_get_dbname(dbsettings)
  username <- .settings_get_username(dbsettings)

  command <- .parse_command(command)

  if (!is.null(command)) {
    fsql <- tempfile(pattern = "sql", fileext = ".sql")
    cat(command, file = fsql)
  } else {
    if (!file.exists(file)) stop("No command provided so expected a file name")
    fsql <- file
  }

  args <- glue::glue('-d {dbname} -U {username} -f {fsql}')
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


#' Create the point counts table if it does not already exist
#'
#' This function sets up a table to store multi-band rasters of integer point
#' counts within voxels. The table has two columns: \code{rid}: an
#' auto-incremented integer record number; \code{rast} raster tiles (aka chips)
#' stored in binary format. There is no field explicitly linking raster records
#' to records in other tables (e.g. the LAS metadata table) since, although
#' the tiles in the database may initially correspond to individual LAS files,
#' this can change if rasters are re-tiled or unioned to optimize queries.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param tablename Name of the raster table to hold point counts in the form
#'   \code{'schema.tablename'}. Defaults to \code{'rasters.point_counts'}.
#'
#' @param username Name of the user (default: 'postgres').
#'
#' @return Invisibly returns a list with the following elements:
#'   \describe{
#'     \item{status}{An integer code: 0 for success (as per \code{system});
#'       1 for skipped (the table already existed); or -1 for error.}
#'     \item{msg}{Message returned by the database.}
#'   }
#'
#' @export
#'
db_create_counts_table <- function(dbsettings,
                                   tablename = "rasters.point_counts",
                                   username = "postgres") {

  command <- glue::glue("CREATE TABLE IF NOT EXISTS \\
                      {tablename} \\
                      (rid serial primary key, rast raster);")

  out <- pg_sql(dbsettings, command)

  status <-
    if (any(stringr::str_detect(out, "ERROR"))) -1
    else if(any(stringr::str_detect(out, "NOTICE.+skipping"))) 1
    else 0

  invisible( list(status = status, msg = out) )
}


#' Create the LAS metadata table if it does not already exist
#'
#' Each record of the metadata table holds summary information derived from an
#' individual LAS file including source file, capture date and times, and point
#' counts and average point density. The table also has a geometry column for
#' the bounding rectangle of each LAS image.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param tablename Name of the table to hold metadata and las tile bounding
#'   polygons in the form \code{'schema.tablename'}. Defaults to
#'   \code{'vectors.las_metadata'}.
#'
#' @return Invisibly returns a list with the following elements:
#'   \describe{
#'     \item{status}{An integer code: 0 for success (as per \code{system});
#'       1 for skipped (the table already existed); or -1 for error.}
#'     \item{msg}{Message returned by the database.}
#'   }
#'
#' @export
#'
db_create_metadata_table <- function(dbsettings,
                                     tablename = "vectors.las_metadata") {
  command <- glue::glue(
    "CREATE TABLE IF NOT EXISTS \\
    {tablename} (\\
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

  out <- pg_sql(dbsettings, command)

  status <-
    if (any(stringr::str_detect(out, "ERROR"))) -1
  else if(any(stringr::str_detect(out, "NOTICE.+skipping"))) 1
  else 0

  invisible( list(status = status, msg = out) )
}


#' Import a raster of point counts and its metadata into the database
#'
#' @param las.path The path and filename of the LAS source file. This can be
#'   an uncompressed (\code{.las}) or compressed \code{.laz} or \code{.zip}
#'   file.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param counts.tablename The name of the table in which to store the raster of
#'   point counts within vertical layers. Should be in the form
#'   \code{'schema.table'}. Defaults to \code{'rasters.tmp_load'}.
#'
#' @param metadata.tablename The name of the table in which to store the summary
#'   data and bounding polygon for the LAS image. Should be in the form
#'   \code{'schema.table'}. Defaults to \code{'vectors.las_metadata'}.
#'
#' @param epsg.code The EPSG code for the coordinate reference system to apply
#'   to data. The point cloud will be re-projected as necessary, prior to
#'   deriving point counts and other data. The default EPSG code is 3308
#'   (New South Wales Lambert projection / GDA94).
#'
#' @export
#'
db_import_las <- function(las.path,
                          dbsettings,
                          counts.tablename = "rasters.tmp_load",
                          metadata.tablename = "vectors.las_metadata",
                          epsg.code = 3308) {

  message("Reading data and normalizing point heights")
  las <- prepare_tile(las.path)

  message("Reprojecting point cloud and removing any flight line overlap")
  las <- las %>%
    reproject_tile(epsg.code) %>%
    remove_flightline_overlap()

  message("Importing point counts for strata")
  counts <- get_stratum_counts(las, StrataCERMB)

  pg_load_raster(counts, epsg = epsg.code,
                 dbsettings,
                 tablename = counts.tablename,
                 tilew = ncol(counts),
                 tileh = nrow(counts))

  message("Merging new and existing rasters")
  pg_sql(dbsettings, command = CERMBlidarpostgis::SQL_MergeImportRaster)

  message("Importing LAS metadata")
  db_load_tile_metadata(las, las.path,
                        dbsettings,
                        tablename = metadata.tablename)
}


#' Check if a LAS image has already been imported
#'
#' @export
#'
db_find_las <- function(las.path, dbsettings) {
  stop("Sorry - not working yet")

  fname <- .file_remove_extension( .file_from_path(las.path) )
}


#' Import a raster into the database
#'
#' @param r The raster to import.
#'
#' @param label A label to use to construct a file name for writing the raster
#'   to disk prior to import. This will be stored in the 'filename' column of
#'   the raster table.
#'
#' @param epsg EPSG code for the raster projection.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param tablename Name of the raster table in the form \code{'schema.tablename'}.
#'
#' @param replace If \code{TRUE} and the table exists, it will be dropped and
#'   recreated. If \code{FALSE} (default) the data will be appended to an
#'   existing table if present, otherwise a new table will be created.
#'
#' @param tilew Width (pixels) of raster tiles as stored in the database
#'   table. Refer to PostGIS documentation for an explanation. If \code{NULL}
#'   (default) this will be set to the width of the input raster.
#'
#' @param tileh height (pixels) of raster tiles as stored in the database
#'   table. Refer to PostGIS documentation for an explanation. If \code{NULL}
#'   (default) this will be set to the height of the input raster.
#'
#' @param flags Other options to be passed to the PostgreSQL raster2pgsql
#'   program. Default is \code{"-M -Y"} to run vacuum analyzed (-M) and
#'   use COPY instead of INSERT statements for speed (-Y). See
#'   raster2pgsql documentation for details of other options.
#'
#' @export
#'
pg_load_raster <- function(r, epsg,
                           dbsettings,
                           tablename,
                           replace = FALSE,
                           tilew = NULL, tileh = NULL,
                           flags = "-M -Y") {

  dbname <- .settings_get_dbname(dbsettings)
  username <- .settings_get_username(dbsettings)
  PSQL <- .settings_get_psql(dbsettings)
  R2P <- .settings_get_r2p(dbsettings)

  is.tbl <- pg_table_exists(dbsettings, tablename)
  if (replace) {
    in.mode <- "-d"
  } else if (is.tbl) {
    in.mode <- "-a"
  } else {
    in.mode <- "-c"
  }

  ftif <- tempfile(pattern = "counts", fileext = ".tif")
  fsql <- tempfile(pattern = "sql", fileext = ".txt")

  raster::writeRaster(r, filename = ftif,
                      format = "GTiff",
                      overwrite = TRUE)

  if (is.null(tilew)) tilew <- ncol(r)
  if (is.null(tileh)) tileh <- nrow(r)

  stopifnot(tilew > 0, tileh > 0)

  args <- glue::glue('{in.mode} -s {epsg} -t {tilew}x{tilew} {flags} {ftif} {tablename}')
  system2(command = R2P, args = args, stdout = fsql)

  args <- glue::glue('-d {dbname} -U {username} -f {fsql}')
  system2(command = PSQL, args = args)

  unlink(ftif)
  unlink(fsql)
}


#' Import LAS tile metadata into the database
#'
#' @param las A LAS object.
#'
#' @param filename Path or filename from which the LAS object was read.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param tablename Name of the raster table in the form \code{'schema.tablename'}.
#'
#' @export
#'
db_load_tile_metadata <- function(las, filename,
                                  dbsettings, tablename) {

  if (!pg_table_exists(dbsettings, tablename)) {
    msg <- glue::glue("Table {tablename} not found in database {dbname}")
    stop(msg)
  }

  epsgcode <- lidR::epsg(las)

  filename <- .file_from_path(filename)
  scantimes <- CERMBlidar::get_scan_times(las, by = "all")

  pcounts <- CERMBlidar::get_class_frequencies(las)
  ptotal <- Reduce(sum, pcounts)

  nflightlines <- length(unique(las@data$flightlineID))

  bounds <- CERMBlidar::get_las_bounds(las, "sf")
  wkt <- sf::st_as_text(bounds)
  area <- sf::st_area(bounds)

  tformat <- function(timestamp) format(timestamp, usetz = TRUE)

  command <- glue::glue(
    "insert into {tablename} \\
    (filename, capture_start, capture_end, \\
    area, \\
    point_density, \\
    npts_ground, npts_veg, npts_building, npts_water, npts_other, npts_total, \\
    nflightlines,
    bounds) \\
    values(\\
    '{filename}', \\
    '{tformat(scantimes[1,1])}', \\
    '{tformat(scantimes[1,2])}', \\
    {area}, \\
    {ptotal / area}, \\
    {pcounts$ground}, \\
    {pcounts$veg}, \\
    {pcounts$building}, \\
    {pcounts$water}, \\
    {pcounts$other}, \\
    {ptotal}, \\
    {nflightlines}, \\
    ST_GeomFromText('{wkt}', {epsgcode}) );
    ")

  pg_sql(dbsettings, command)
}


#' Check if a table exists in the given database
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param tablename Name of the table in the form \code{schema.tablename}.
#'
#' @return \code{TRUE} if the table was found; \code{FALSE} otherwise.
#'
#' @examples
#' \dontrun{
#' dbsettings <- connect_to_database(...)
#' pg_table_exists(dbsettings, "postgis.bega")
#' }
#'
#' @export
#'
pg_table_exists <- function(dbsettings, tablename) {
  POOL <- .settings_get_pool(dbsettings)

  command <- glue::glue("select count(*) as n from {tablename};")

  o <- options(warn = -1, show.error.messages = FALSE)
  x <- try(pool::dbGetQuery(POOL, command))
  options(o)

  !is.null(x)
}


# Extract filename and extension from a path
.file_from_path <- function(path) {
  stringr::str_extract(path, "[\\w\\.\\-]+$")
}

# Remove extension from filename
.file_remove_extension <- function(filename) {
  filename <- filename[1]
  ext <- "\\.\\w+$"

  if (stringr::str_detect(filename, ext))
    pos <- stringr::str_locate(filename, ext)[1,1] - 1
  else
    pos <- -1

  stringr::str_sub(filename, 1, pos)
}
