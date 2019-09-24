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


#' Create the two point counts tables if they do not already exist
#'
#' This function sets up two tables to store multi-band rasters of integer point
#' counts within voxels. Each table has two columns: \code{rid}: an
#' auto-incremented integer record number; \code{rast} raster tiles (i.e.
#' PostGIS storage tiles rather than LAS images). The first of the two tables
#' holds rasters derived from individual LAS images. The PostGIS tile size for
#' each record corresponds to the dimensions of the input poinit counts raster.
#' The second table, identified by the suffix \code{'_union'} holds rasters that
#' have been mosaiced by summing the values of overlapping edge pixels.
#'
#' The reason for holding duplicate data is to allow for deletion and
#' re-importing of data (e.g. to fix artefacts in a particular raster) that
#' would otherwise be difficult with just the mosaiced data. There is no field
#' explicitly linking raster records to records in other tables (e.g. the LAS
#' metadata table). Such relationships are found via spatial queries.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param tablename Name of the raster table to hold point counts for individual
#'   (not mosaiced) LAS images in the form \code{'schema.tablename'}. Defaults
#'   to \code{'rasters.point_counts'}. The table for mosaiced rasters will be
#'   given the same name plus the suffix \code{'_union'}.
#'
#' @param username Name of the user (default: 'postgres').
#'
#' @export
#'
db_create_counts_table <- function(dbsettings,
                                   tablename = "rasters.point_counts",
                                   username = "postgres") {

  command <- glue::glue("CREATE TABLE IF NOT EXISTS \\
                        {tablename} \\
                        (rid serial primary key, rast raster);")

  p <- .settings_get_pool(dbsettings)
  pool::dbExecute(p, command)

  command2 <- glue::glue("CREATE TABLE IF NOT EXISTS \\
                         {tablename}_union \\
                         (rid serial primary key, rast raster);")

  # Capture integer result to avoid returning a value that is of no
  # use to the caller
  dummy <- pool::dbExecute(p, command2)
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

  p <- .settings_get_pool(dbsettings)

  # Capture integer result to avoid returning a value that is of no
  # use to the caller
  dummy <- pool::dbExecute(p, command2)
}


#' Import a raster of point counts and its metadata into the database
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param las.path The path and filename of the LAS source file. This can be
#'   an uncompressed (\code{.las}) or compressed \code{.laz} or \code{.zip}
#'   file.
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
db_import_las <- function(dbsettings,
                          las.path,
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

  # Load point counts for this tile into the temp table
  pg_load_raster(dbsettings,
                 counts, epsg = epsg.code,
                 tablename = counts.tablename,
                 tilew = ncol(counts),
                 tileh = nrow(counts))

  message("Merging new and existing rasters")
  p <- .settings_get_pool(dbsettings)
  pool::dbExecute(p, CERMBlidarpostgis::SQL_MergeImportRaster)

  message("Importing LAS metadata")
  db_load_tile_metadata(dbsettings,
                        las, las.path,
                        tablename = metadata.tablename)
}


#' Check if one or more LAS files have been imported into the database
#'
#' When a LAS image is imported, the name of the file from which it was read is
#' recorded in the \code{vectors.las_metadata} table of the database. This
#' function queries that table to see which, if any, of the names provided
#' in the \code{filenames} argument are present in that table.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param filenames A character vector of one or more file names. These can be
#'   full paths or file names with or without extensions. File extensions
#'   (e.g. \code{.las; .LAZ; .zip}) and are ignored for comparison purposes, as
#'   is case.
#'
#' @return A logical vector with the input file names as element names and
#'   \code{TRUE} values indicating files that have already been imported.
#'
#' @export
#'
db_lasfile_imported <- function(dbsettings, filenames) {
  if (!pg_table_exists(dbsettings, "las_metadata")) {
    # Empty database
    rep(FALSE, length(filenames))

  } else {
    sapply(filenames, function(fname) {
      fname <- .file_remove_extension( .file_from_path(fname) )

      POOL <- .settings_get_pool(dbsettings)

      command <- glue::glue("SELECT COUNT(*) AS nrecs
                           FROM vectors.las_metadata
                           WHERE filename ILIKE '{fname}%'")

      x <- pool::dbGetQuery(POOL, command)
      x$nrecs[1] > 0
    })
  }
}


#' Import a raster into the database
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param r The raster to import.
#'
#' @param label A label to use to construct a file name for writing the raster
#'   to disk prior to import. This will be stored in the 'filename' column of
#'   the raster table.
#'
#' @param epsg EPSG code for the raster projection.
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
pg_load_raster <- function(dbsettings,
                           r, epsg,
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
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param las A LAS object.
#'
#' @param filename Path or filename from which the LAS object was read.
#'
#' @param tablename Name of the raster table in the form \code{'schema.tablename'}.
#'
#' @export
#'
db_load_tile_metadata <- function(dbsettings,
                                  las, filename,
                                  tablename) {

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

  p <- .settings_get_pool(dbsettings)
  dummy <- pool::dbExecute(p, command)
}


#' Export raster point count data that intersects with one or more features
#'
#' This function takes a set of features (points, lines or polygons), identifies
#' the raster tiles of point count data that the features intersect with, and
#' exports these to a GeoTIFF file.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{connect_to_database}}.
#'
#' @param geom The feature(s) to intersect.
#'
#' @param outpath A path and filename for the output GeoTIFF file.
#'
#' @param default.epsg EPSG code to assume for the features if none has been
#'   assigned (e.g. with \code{sf::st_crs}).
#'
#' @return A raster stack linked to the exported GeoTIFF file.
#'
#' @export
#'
db_export_point_counts <- function(dbsettings, geom, outpath,
                                default.epsg = 3308) {

  if (inherits(geom, what = c("sfc", "sfg")))
    g <- geom
  else if (inherits(geom, what = "sf")) {
    isgeom <- sapply(geom, inherits, what = "sfc")
    if (!sum(isgeom) == 1)
      stop("Failed to find geometry column in sf data frame")
    g <- geom[, isgeom, drop=TRUE]
  }

  g.crs <- sf::st_crs(g)
  if (!is.na(g.crs)) epsg <- g.crs$epsg
  else epsg <- default.epsg

  g.values <- sapply(g, function(gg) {
    glue::glue("ST_GeomFromText('{sf::st_as_text(gg)}', {epsg})")
  })

  g.values <- paste( paste("(", g.values, ")"), collapse = ",")

  command <- glue::glue("
    BEGIN;
    --------
    CREATE TEMPORARY TABLE tmp_features (
      feature geometry(GEOMETRY, {epsg}) NOT NULL
    );
    --------
    INSERT INTO tmp_features (feature)
      VALUES {g.values};
    --------
    CREATE TEMPORARY TABLE tmp_export_rast AS
      SELECT lo_from_bytea(0, ST_AsGDALRaster(ST_Union(u.rast), 'GTiff')) AS loid
      FROM rasters.point_counts_union AS u, tmp_features as f
      WHERE ST_Intersects(u.rast, f.feature);
    --------
    SELECT lo_export(loid, '{outpath}')
      FROM tmp_export_rast;
    --------
    SELECT lo_unlink(loid)
      FROM tmp_export_rast;
    --------
    DROP TABLE tmp_features;
    DROP TABLE tmp_export_rast;
    --------
    COMMIT;
  ")

  the.pool <- .settings_get_pool(dbsettings)
  pool::dbExecute(the.pool, command)

  if (file.exists(outpath)) {
    r <- raster::stack(outpath)
    names(r) <- paste("height", 1:raster::nlayers(r), sep = ".")
    r
  } else {
    warning("Output GeoTIFF file was not created")
    NULL
  }
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
#' pg_table_exists(dbsettings, "rasters.point_counts_union")
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
