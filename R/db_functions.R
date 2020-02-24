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
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
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
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @examples
#' \dontrun{
#' Sys.setenv(PGPASSWORD = "cermb")
#'
#' dbsettings <- db_connect_postgis("cermb_lidar")
#'
#' pg_sql(dbsettings, "SELECT COUNT(*) AS NRECS FROM FOO;")
#' }
#'
#' @export
#'
pg_sql <- function(dbsettings, command = NULL, file = NULL, quiet = TRUE) {

  command <- .parse_command(command)

  if (!is.null(command)) {
    fsql <- tempfile(pattern = "sql", fileext = ".sql")
    cat(command, file = fsql)
  } else {
    if (!file.exists(file)) stop("No command provided so expected a file name")
    fsql <- file
  }

  args <- glue::glue('-d {dbsettings$DBNAME} -U {dbsettings$USERNAME} -f {fsql}')
  out <- system2(command = dbsettings$PSQL, args = args, stdout = TRUE)

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


#' Import data from one or more LAS files.
#'
#' First, each LAS tile is read from disk and the function checks that it is
#' located in one of the map zones supported by the database (each zone's data
#' is stored in a separate schema). Next, point heights are normalized, any
#' overlap between flight lines is removed, and the counts of vegetation, ground
#' and water points are rasterized as vertical layers defined in
#' \code{CERMBlidar::CERMBstrata}. The point count raster layers are then
#' imported into the database along with meta-data for tile extent, point
#' density, etc. Optionally, building points can also be extracted from the LAS
#' tile and imported.
#'
#' Point counts for vertical layers of vegetation are stored in the database
#' raster table \code{point_counts}, with a raster record for each imported LAS
#' tile. Typically there will be a lower density of points in edge cells of a
#' raster than in interior cells due to the relative position of LAS tile
#' boundaries and raster boundaries. This can be resolved by edge-merging
#' adjacent rasters, summing overlapping cells (e.g. using the PostGIS ST_UNION
#' operation with the 'SUM' option). The original intention was to do this by
#' default as part of the import process, and store an edge-merged copy of the
#' raster point counts data in a second database table
#' \code{point_counts_union}. However, we found that the time taken by the
#' merging operation became excessive as the number of rasters in the database
#' grew, although we are yet to understand exactly why this is the case. Merging
#' can be enabled by setting the \code{union.rasters} argument to \code{TRUE}
#' (default is \code{FALSE}) \strong{but this is not recommended.} Note that when
#' exporting point count data with function \code{db_export_stratum_counts}, the
#' rasters for adjacent LAS tiles will be edge-merged.
#'
#' @param dbsettings A named list of database connection settings as returned
#'   by \code{\link{db_connect_postgis}}.
#'
#' @param las.paths The paths and filenames of the LAS source files. This can
#'   be an uncompressed (\code{.las}) or compressed \code{.laz} or \code{.zip}
#'   file.
#'
#' @param dem.paths The paths and file names of DEM (digital elevation model)
#'   rasters corresponding to the LAS source files. If a vector of paths is
#'   provided it must be the same length as \code{las.paths}. For each element
#'   that specifies the path to a raster file (e.g. GeoTIFF) or zipped raster
#'   file, the DEM data will read and used to normalize the point heights for
#'   the corresponding LAS file. Where an element is \code{NA} or
#'   an empty string, point heights for the corresponding LAS file will be
#'   normalized using Delaunay triangulation. If this argument is set to
#'   \code{NULL} (default), triangulation will be used for all LAS files.
#'
#' @param mapnames Specifies the mapsheet names to store for the imported
#'   LAS tiles. If \code{NULL} (default), mapsheet names are extracted from the
#'   leading alphabetic portion of the LAS file names. Alternatively it can be
#'   a character vector of either length 1 (same name for all LAS tiles) or
#'   length equal to that of \code{las.paths}.
#'
#' @param default.epsgs Integer EPSG codes for each of the imported LAS tiles.
#'   If \code{NULL} (default), EPSG codes will be read from the LAS headers.
#'   Sometimes (e.g. class C1 LAS files) no coordinate reference sytem is
#'   defined in the LAS header, so it is necessary to set the code manually.
#'
#' @param purpose Either \code{'general'} (default) for general purpose imagery
#'   or \code{'postfire'} for specially flown, post-fire imagery. May be
#'   abbreviated.
#'
#' @param check.overlap If \code{TRUE} (default), images will be checked for
#'   overlap between flight lines using the function
#'   \code{remove_flightline_overlap} in the \code{CERMBlidar} package. If an
#'   image fails the overlap check (i.e. it appears to have overlap and this
#'   cannot be removed) it will be skipped. Setting this argument to
#'   \code{FALSE} disables overlap checking. This is useful for images with
#'   unusual configurations of flight lines that cause a false positive overlap
#'   check.
#'
#' @param buildings If \code{TRUE}, points classified as buildings (class 6)
#'   will be imported into the \code{building_points} database table. The
#'   default (\code{FALSE}) is to not import building points.
#'
#' @param union.rasters \strong{See Details for why you probably do not want to
#'   use this argument.} If \code{TRUE}, raster point counts derived from the
#'   LAS images will be loaded into both the \code{point_counts} table as a
#'   single record and the \code{point_counts_union} table where data for
#'   spatially adjacent, overlapping rasters are merged. If \code{FALSE}
#'   (default), raster point counts are only loaded into the \code{point_counts}
#'   table.
#'
#' @param union.batch If \code{union.rasters = TRUE}, this defines how many
#'   rasters to load before merging data into the \code{point_counts_union}
#'   table. The default, and minimum allowable, value is 1.
#'
#' @return A vector of integer values giving the result for each input LAS file,
#'   where 1 means successfully imported; 0 means skipped because previously
#'   imported; -1 means not imported due to error (usually a problem with
#'   resolving flight line overlap).
#'
#' @examples
#' \dontrun{
#' # Establish connection
#' Sys.setenv(PGPASSWORD = "mypassword")
#' DB <- db_connect_postgis("cermb_lidar")
#'
#' # Import a set of LAS files. Point heights will be set relative
#' # to ground level via Delaunay triangulation.
#' LAS.FILES <- dir("c:/somewhere/LAS", pattern = "zip$", full.names = TRUE)
#' imported <- db_import_las(DB, LAS.FILES)
#' cat(sum(imported == 1), "files were imported\n",
#'     sum(imported == 0), "files already present were skipped\n",
#'     sum(imported == -1), "files failed to be imported\n")
#'
#' # Import a set of LAS files with corresponding DEM (raster elevation)
#' # files to be used to normalize point heights.
#' LAS.FILES <- dir("c:/somewhere/LAS", pattern = "zip$", full.names = TRUE)
#' DEM.FILES <- dir("c:/somewhere/DEM", pattern = "zip$", full.names = TRUE)
#' imported <- db_import_las(DB, LAS.FILES, DEM.FILES)
#'
#' }
#'
#' @export
#'
db_import_las <- function(dbsettings,
                          las.paths,
                          dem.paths = NULL,
                          mapnames = NULL,
                          default.epsgs = NULL,
                          purpose = c("general", "postfire"),
                          check.overlap = TRUE,
                          buildings = FALSE,
                          union.rasters = FALSE,
                          union.batch = 1) {

  p <- .get_pool(dbsettings)

  purpose <- match.arg(purpose)

  if (union.rasters) {
    warning("Beware!!!",
            "Merging of rasters is presently experimental and will probably",
            "end in tears", immediate. = TRUE)

    if (union.batch < 1) stop("The argument union.batch must be >= 1")
  }

  fn_check_exists <- function(paths, label) {
    x <- sapply(paths, file.exists)
    if (any(!x)) {
      x <- x[!x]
      if (length(x) <= 5) {
        msg <- paste("The following", label, "files cannot be found:")
        msg <- paste(msg, paste(names(x), collapse = "\n"), sep = "\n")
      } else {
        msg <- paste(length(x), label, "files cannot be found including:")
        msg <- paste(msg, paste(head(names(x), 5), collapse = "\n"), sep = "\n")
      }
      stop(msg)
    }
  }

  fn_check_exists(las.paths, "LAS")

  if (!is.null(dem.paths)) {
    if (length(las.paths) != length(dem.paths)) {
      stop("When dem.paths is not NULL it must be the same length as las.paths")
    }

    non.empty.paths <- stringr::str_subset(dem.paths, "[^\\s]")
    fn_check_exists(non.empty.paths, "DEM")

    # Set any empty paths to NA
    ii <- stringr::str_length(stringr::str_trim(dem.paths)) == 0
    dem.paths[ii] <- NA
  }


  imported <- rep(0, length(las.paths))
  n.union <- 0
  Nlas <- length(las.paths)

  for (i in 1:Nlas) {
    las.file <- las.paths[i]

    if (db_lasfile_imported(dbsettings, las.file)) {
      message("Skipping previously imported file: ", .file_from_path(las.file))
    } else {
      msg <- glue::glue("{i}/{Nlas} {.file_from_path(las.file)}")
      message(msg)

      dem.file <- dem.paths[i]
      if (!is.null(dem.file)) {
        if (is.na(dem.file)) dem.file <- NULL
      }

      if (is.null(mapnames)) {
        mapname <- stringr::str_extract(.file_from_path(las.file), "^[A-Za-z]+")
      } else if (length(mapnames) == 1) {
        mapname <- mapnames
      } else {
        mapname <- mapnames[i]
      }

      if (is.null(default.epsgs)) {
        default.epsgcode <- NULL
      } else if (length(default.epsgs) == 1) {
        default.epsgcode <- default.epsgs
      } else {
        default.epsgcode <- default.epsgs[i]
      }

      tryCatch({
        .do_import_las(dbsettings, las.file, dem.file,
                       default.epsgcode, mapname, purpose,
                       check.overlap, buildings)
        imported[i] <- 1

        if (union.rasters) {
          n.union <- n.union + 1
          if (n.union >= union.batch) {
            .do_merge_new_rasters(dbsettings, schema = NULL)
            n.union <- 0
          }
        } else {
          # Not merging rasters so clean out tmp_load tables
          .do_delete_tmp_tables(dbsettings, schema = NULL);
        }
      },
      error = function(e) {
        imported[i] <<- -1
      })
    }
  }

  # If union-ing, process any remaining rasters
  if (union.rasters) {
    .do_merge_new_rasters(dbsettings, schema = NULL)
  }

  imported
}


# Non-exported helper function for db_import_las
#
.do_import_las <- function(dbsettings,
                          las.path,
                          dem.path = NULL,
                          default.epsgcode,
                          mapname,
                          purpose,
                          check.overlap,
                          buildings) {

  message("Reading data and normalizing point heights")

  if (is.null(dem.path))
    las <- CERMBlidar::prepare_tile(las.path, normalize.heights = "tin")
  else
    las <- CERMBlidar::prepare_tile(las.path, normalize.heights = dem.path)

  if (check.overlap) {
    message("Removing any flight line overlap")
    las <- CERMBlidar::remove_flightline_overlap(las)
  } else {
    message("Skipping flight line overlap check")
  }

  message("Importing LAS metadata")
  metadata.id <- db_load_tile_metadata(dbsettings,
                                       las = las,
                                       filename = las.path,
                                       mapname = mapname,
                                       purpose = purpose,
                                       default.epsg = default.epsgcode)

  message("Importing point counts for strata")
  db_load_stratum_counts(dbsettings,
                         las = las,
                         metadata.id = metadata.id,
                         default.epsg = default.epsgcode)

  if (buildings && (6 %in% las@data$Classification)) {
    message("Importing building points")
    db_load_building_points(dbsettings,
                            las = las,
                            metadata.id = metadata.id,
                            default.epsg = default.epsgcode)
  }
}


#' Check if one or more LAS files have been imported into the database
#'
#' When a LAS image is imported, the name of the file from which it was read is
#' recorded in the \code{lidar.las_metadata} table of the database. This
#' function queries that table to see which, if any, of the names provided
#' in the \code{filenames} argument are present in that table.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param filenames A character vector of one or more file names. These can be
#'   full paths or file names with or without extensions. File extensions
#'   (e.g. \code{.las; .LAZ; .zip}) and are ignored for comparison purposes, as
#'   is case.
#'
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @return A logical vector with the input file names as element names and
#'   \code{TRUE} values indicating files that have already been imported.
#'
#' @export
#'
db_lasfile_imported <- function(dbsettings, filenames) {
  p <- .get_pool(dbsettings)
  found <- logical(length(filenames))

  fname <- .file_remove_extension( .file_from_path(filenames) )

  # Search all schemas
  for (schema in dbsettings$schema) {
    tblname <- glue::glue("{schema}.{dbsettings$TABLE_METADATA}")
    command <- glue::glue("SELECT filename FROM {tblname};")
    x <- pool::dbGetQuery(p, command)$filename
    if (length(x) > 0) {
      found <- found | sapply(fname, function(f) tolower(f) %in% tolower(x))
    }
  }

  found
}


#' Import a raster into the database
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
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
#' @return The integer \code{rid} value of the record(s) for the imported
#'   raster.
#'
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @export
#'
pg_load_raster <- function(dbsettings,
                           r, epsg,
                           tablename,
                           replace = FALSE,
                           tilew = NULL, tileh = NULL,
                           flags = "-M -Y") {

  p <- .get_pool(dbsettings)

  is.tbl <- pg_table_exists(dbsettings, tablename)

  if (replace) {
    in.mode <- "-d"
  } else if (is.tbl) {
    in.mode <- "-a"
  } else {
    in.mode <- "-c"
  }

  if (!replace && is.tbl) {
    # Get current rid values for records in this table
    cmd <- glue::glue("select rid from {tablename};")
    existing.recs <- pool::dbGetQuery(p, cmd)
  } else {
    existing.recs <- data.frame(rid = integer(0))
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
  system2(command = dbsettings$R2P, args = args, stdout = fsql)

  args <- glue::glue('-d {dbsettings$DBNAME} -U {dbsettings$USERNAME} -f {fsql}')
  system2(command = dbsettings$PSQL, args = args)

  unlink(ftif)
  unlink(fsql)

  # Return rid values for newly inserted raster recs
  cmd <- glue::glue("select rid from {tablename};")
  all.recs <- pool::dbGetQuery(p, cmd)

  setdiff(all.recs$rid, existing.recs$rid)
}


#' Import LAS tile metadata into the database
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param las A LAS object.
#'
#' @param filename Path or filename from which the LAS object was read.
#'
#' @param mapname Name of the map sheet (assumed to be 100k topographic map) to
#'   assign to this LAS tile.
#'
#' @param purpose Either \code{'general'} (default) for general purpose imagery
#'   or \code{'postfire'} for specially flown, post-fire imagery. May be
#'   abbreviated.
#'
#' @return The integer value of the \code{'id'} field for the newly created
#'   database record.
#'
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @export
#'
db_load_tile_metadata <- function(dbsettings,
                                  las, filename, mapname,
                                  purpose = c("general", "postfire"),
                                  default.epsg = NULL) {

  .check_database_password()

  purpose <- match.arg(purpose)

  bounds <- CERMBlidar::get_las_bounds(las, "sf")
  las.crs <- sf::st_crs(bounds)
  if (is.na(las.crs)) {
    if (is.null(default.epsg))
      stop("Argument default.epsg is NULL and LAS file has no CRS defined")

    epsgcode <- default.epsg
  } else {
    epsgcode <- las.crs$epsg
  }

  schema <- .get_schema_for_epsg(dbsettings, epsgcode)

  tblname <- glue::glue("{schema}.{dbsettings$TABLE_METADATA}")

  if (!pg_table_exists(dbsettings, tblname)) {
    msg <- glue::glue("Table {tblname} not found in database {dbname}")
    stop(msg)
  }

  filename <- .file_from_path(filename)
  filename <- .file_remove_extension(filename)
  scantimes <- CERMBlidar::get_scan_times(las, by = "all")

  pcounts <- CERMBlidar::get_class_frequencies(las)
  ptotal <- Reduce(sum, pcounts)

  nflightlines <- length(unique(las@data$flightlineID))

  wkt <- sf::st_as_text(bounds)
  area <- sf::st_area(bounds)

  command <- glue::glue(
    "insert into {tblname} \\
    (filename, mapname, purpose, capture_start, capture_end,
    area, point_density,
    npts_ground, npts_veg, npts_building, npts_water, npts_other, npts_total,
    nflightlines,
    bounds)
    values (
    '{filename}',
    '{mapname}',
    '{purpose}',
    '{.tformat(scantimes[1,1])}',
    '{.tformat(scantimes[1,2])}',
    {area},
    {ptotal / area},
    {pcounts$ground},
    {pcounts$veg},
    {pcounts$building},
    {pcounts$water},
    {pcounts$other},
    {ptotal},
    {nflightlines},
    ST_GeomFromText('{wkt}', {epsgcode}) );
    ")

  p <- .get_pool(dbsettings)
  pool::dbExecute(p, command)

  # Return id value of the record just created
  res <- pool::dbGetQuery(p, glue::glue("select id from {tblname} \\
                                        where filename = '{filename}'"))

  res$id[1]
}


#' Rasterize and import points counts
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param las A LAS object.
#'
#' @param metadata.id Integer ID value of the corresponding record in the
#'   \code{'las_metadata'} table.
#'
#' @export
#'
db_load_stratum_counts <- function(dbsettings,
                                   las,
                                   metadata.id,
                                   default.epsg = NULL) {

  .check_database_password()

  counts <- CERMBlidar::get_stratum_counts(las, CERMBlidar::StrataCERMB)

  epsgcode <- lidR::epsg(las)
  if (is.na(epsgcode) | epsgcode == 0) {
    if (is.null(default.epsg))
      stop("Argument default.epsg is NULL and LAS file has no CRS defined")

    epsgcode <- default.epsg
  }

  schema <- .get_schema_for_epsg(dbsettings, epsgcode)

  # Load point counts for this tile into the temp table. If the table does
  # not exist it will be created, otherwise the new data will be appended
  # to the existing table.
  new.rids <- pg_load_raster(dbsettings,
                             counts,
                             epsg = epsgcode,
                             tablename = glue::glue("{schema}.tmp_load"),
                             replace = FALSE,
                             tilew = ncol(counts),
                             tileh = nrow(counts))

  # Copy the new data into the point_counts table
  new.rids.clause <- paste(new.rids, sep = ", ")
  cmd <- glue::glue("
      insert into {schema}.point_counts (meta_id, rast)
        select {metadata.id} as meta_id, rast from {schema}.tmp_load
        where rid in ({new.rids.clause});")

  p <- .get_pool(dbsettings)
  pool::dbExecute(p, cmd)
}


# Non-exported function to merge newly imported rasters in the tmp_load
# table into point_counts_union
#
.do_merge_new_rasters <- function(dbsettings, schema = NULL) {
  if (is.null(schema)) {
    # check all schemas
    schemas <- dbsettings$schema
  } else {
    schemas <- schema
  }

  for (schema in schemas) {
    tmp_load <- glue::glue("{schema}.tmp_load")
    tmp_union <- glue::glue("{schema}.tmp_union")

    if (pg_table_exists(dbsettings, tmp_load)) {
      p <- .get_pool(dbsettings)

      # Check that there is some newly imported data
      cmd <- glue::glue("select count(rast) as N from {tmp_load};")
      x <- pool::dbGetQuery(p, cmd)

      if (x$N > 0) {
        # Merge the new and existing rasters
        cmd <- glue::glue("
        -- Identify existing rasters that overlap the new data
        create or replace view {schema}.overlaps as
          select pcu.rid from
            {schema}.point_counts_union as pcu, {tmp_load} as tl
            where st_intersects(pcu.rast, tl.rast);

        -- Union overlapping rasters with new data, summing overlap values
        create table if not exists {tmp_union} (rast raster);

        -- In case the tmp_union table already existed, clear all rows
        truncate table {tmp_union};

        insert into {tmp_union}
          select ST_Union(r.rast, 'SUM') as rast from
            (select rast from {schema}.point_counts_union
            where rid in (select rid from {schema}.overlaps)
            union all
            select rast from {tmp_load}) as r;

        -- Delete overlapping rasters from main table
        delete from {schema}.point_counts_union
          where rid in (select rid from {schema}.overlaps);

        -- Insert updated data
        select DropRasterConstraints('{schema}'::name, 'point_counts_union'::name, 'rast'::name);

        insert into {schema}.point_counts_union (rast)
          select ST_Tile(rast, 100, 100) as raster
          from {tmp_union};

        select AddRasterConstraints('{schema}'::name, 'point_counts_union'::name, 'rast'::name);

        -- Delete records from the temporary tables
        truncate table {tmp_load};
        truncate table {tmp_union}; ")

        msg <- glue::glue("Merging new and existing rasters in {schema}")
        message(msg)

        p <- .get_pool(dbsettings)
        pool::dbExecute(p, cmd)
        pool::dbExecute(p, glue::glue("VACUUM ANALYZE {schema}.point_counts;"))
        pool::dbExecute(p, glue::glue("VACUUM ANALYZE {schema}.point_counts_union;"))
      }
    }
  }
}


.do_delete_tmp_tables <- function(dbsettings, schema = NULL) {
  if (is.null(schema)) {
    # check all schemas
    schemas <- dbsettings$schema
  } else {
    schemas <- schema
  }

  for (schema in schemas) {
    tmp_load <- glue::glue("{schema}.tmp_load")
    tmp_union <- glue::glue("{schema}.tmp_union")

    command <- glue::glue("drop table if exists {tmp_load};
                           drop table if exists {tmp_union};")

    p <- .get_pool(dbsettings)
    pool::dbExecute(p, command)
  }
}


#' Import building points
#'
#' @export
#'
db_load_building_points <- function(dbsettings,
                                    las,
                                    metadata.id,
                                    default.epsg = NULL) {

  .check_database_password()

  epsgcode <- lidR::epsg(las)
  if (is.na(epsgcode) | epsgcode == 0) {
    if (is.null(default.epsg))
      stop("Argument default.epsg is NULL and LAS file has no CRS defined")

    epsgcode <- default.epsg
  }

  schema <- .get_schema_for_epsg(dbsettings, epsgcode)

  p <- .get_pool(dbsettings)

  dat <- CERMBlidar::get_building_points(las)
  if (nrow(dat) > 0) {
    values <- glue::glue_collapse(
      glue::glue("({metadata.id}, {dat$Z}, \\
                  ST_GeomFromText('{sf::st_as_text(dat$geometry)}', {epsgcode}))"),
      sep = ", "
    )

    command <- glue::glue("INSERT INTO {schema}.building_points \\
                            (meta_id, height, geom) VALUES {values};")

    pool::dbExecute(p, command)
  }
}


#' Query bounding polygons of imported LAS images
#'
#' This function queries the \code{'las_metadata'} table with optional
#' subsetting based on LAS image file names and/or capture dates. It returns an
#' \code{'sf'} spatial data frame containing the source file name, start and end
#' capture dates and times, and bounding rectangle of the point cloud as a
#' polygon. If no file name or date constraints are specified, all records in
#' the table are returned.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param mapnames An optional list of map names. The default (\code{NULL}) is to
#'   include all map names. Case is ignored.
#'
#' @param with.purpose One of \code{'general'} (default) for general purpose
#'   imagery; \code{'postfire'} for specially flown, post-fire imagery; or
#'   \code{'all'} for all imagery. May be abbreviated.
#'
#' @param file.pattern A string pattern to match LAS source file names. Matching
#'   is done ignoring case. The pattern can be any of the following:
#'   \itemize{
#'     \item{A plain string - to return file names containing this string;}
#'     \item{An SQL string expression with \code{'\%'} wildcard - e.g. \code{'Bega\%5946\%';}}
#'     \item{A regular expression - e.g. \code{'^Bega.+5946'}.}
#'   }

#'   The default (\code{NULL}) means match any file name.
#'
#' @param capture.start Beginning of time window for LAS images. Can be
#'   specified as a character string or an object of class \code{Date},
#'   \code{POSIXct} or \code{POSIXlt}. The default \code{NULL} means no start
#'   time constraint.
#'
#' @param capture.end End of time window for LAS images. Can be
#'   specified as a character string or an object of class \code{Date},
#'   \code{POSIXct} or \code{POSIXlt}. The default \code{NULL} means no end
#'   time constraint.
#'
#' @return An \code{'sf'} spatial data frame containing the source file name,
#'   start and end capture dates and times, and bounding rectangle of the point
#'   cloud as a polygon.
#'
#' @export
#'
db_get_las_bounds <- function(dbsettings,
                              mapnames = NULL,
                              with.purpose = c("general", "postfire", "all"),
                              file.pattern = NULL,
                              capture.start = NULL,
                              capture.end = NULL) {

  file.pattern <- .first_elem(file.pattern)
  capture.start <- .first_elem(capture.start)
  capture.end <- .first_elem(capture.end)

  with.purpose <- match.arg(with.purpose)

  where.cond <- switch(
    with.purpose,
    general = "purpose = 'general'",
    postfire = "purpose = 'postfire'",
    all = NULL
  )

  if (length(mapnames) > 0) {
    mapnames <- paste(.single_quote(mapnames), collapse = ", ")
    where.cond <- c(where.cond, glue::glue("mapname IN ({mapnames})"))
  }

  if (!is.null(file.pattern)) {
    if (stringr::str_detect(file.pattern, "%"))
      where.cond <- c(where.cond, glue::glue("filename ILIKE '{file.pattern}'"))
    else if (stringr::str_detect(file.pattern, "[\\*\\+\\[\\]\\^\\$]+"))
      where.cond <- c(where.cond, glue::glue("filename ~* '{file.pattern}'"))
    else
      where.cond <- c(where.cond, glue::glue("filename ILIKE '%{file.pattern}%'"))
  }

  if (!is.null(capture.start)) {
    where.cond <- c(where.cond, glue::glue("capture_start >= '{.tformat(capture.start)}'"))
  }

  if (!is.null(capture.end)) {
    where.cond <- c(where.cond, glue::glue("capture_end <= '{.tformat(capture.end)}'"))
  }

  if (!is.null(where.cond)) {
    where.cond <- paste("WHERE", paste(where.cond, collapse=" AND "))
  } else {
    where.cond <- ""
  }

  command <- glue::glue("SELECT mapname, filename, capture_start, capture_end,
                        ST_AsEWKT(bounds) AS geometry
                        FROM {dbsettings$TABLE_METADATA}
                        {where.cond};")

  p <- .get_pool(dbsettings)
  res <- pool::dbGetQuery(p, command)

  if (nrow(res) > 0) {
    res$geometry <- sf::st_as_sfc(res$geometry)
  }
  else {
    # Empty result
    res <- data.frame(filename = character(0),
                      capture_start = as.POSIXct(character(0), tz = "UTC"),
                      capture_end = as.POSIXct(character(0), tz = "UTC"),
                      sf::st_sfc())
  }

  sf::st_sf(res)
}


#' Export point count rasters that intersect query features
#'
#' This function takes a set of features (points, lines, polygons or an extent
#' in the form of a matrix or raster Extent object), identifies the raster tiles
#' of point count data that the features intersect with, and exports these to a
#' GeoTIFF file. Optionally, the data to export can be restricted to a specified
#' time period. If any parts of the query area are covered by LiDAR data for two
#' or more dates, the data to export is chosen based on the
#' \code{overlap.action} and \code{overlap.yearorder} parameters.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param geom The feature(s) to intersect. Can also be an \code{Extent} object
#'   or a 2x2 matrix where the first row is X min,max and the second row is
#'   Y min,max.
#'
#' @param outpath A path and filename for the output GeoTIFF file.
#'
#' @param with.purpose Either \code{'general'} (default) to export rasters for
#'   general purpose imagery or \code{'postfire'} to export rasters for
#'   specially flown, post-fire imagery. May be abbreviated.
#'
#' @param time.interval If provided, only export data for rasters where the
#'   scan time (capture_start and capture_end fields) overlap the given interval.
#'   Can be provided as either a vector of one or more years as four-digit integers,
#'   or a \code{lubridate::interval} object.
#'
#' @param default.epsg EPSG code to assume for the query feature(s). Ignored if
#'   the features are \code{sf} or \code{raster::Extent} objects with a coordinate
#'   reference system defined.
#'
#' @param overlap.action A character string specifying how to choose between
#'   overlapping tiles. Two tiles are considered to be overlapping if the
#'   centroid of the first tile lies within the bounds of the second tile. In
#'   such cases, only one of the tiles should be exported, otherwise the UNION
#'   SUM operation that is applied to merge edge pixels of adjacent rasters will
#'   inadvertently sum the values of the overlapping tiles. Options are:
#'   \code{'latest'} (default) to choose the most recent tile; \code{'earliest'}
#'   to choose the earliest tile; \code{'year'} to apply a preference order of
#'   years specified via the \code{overlap.yearorder} parameter; \code{'fail'}
#'   to abort the export process and issue an error message if overlapping tiles
#'   are detected.
#'
#' @param overlap.yearorder A vector of one or more four digit year numbers
#'   specifying the preference order to apply when overlapping LAS tiles are
#'   detected. Only used if parameter \code{overlap.action} is set to
#'   \code{'year'}, The first element in the vector is the most preferred.
#'   Overlapping tiles for years that are not specified in this vector will not
#'   be exported.
#'
#' @return A list with the following elments:
#'   \describe{
#'     \item{counts}{A RasterStack linked to the GeoTIFF file exported by the
#'       function, with layer names corresonding to those defined in the
#'       StrataCERMB lookup table from the CERMBlidar package.}
#'     \item{dates}{A RasterLayer having the same row and column dimensions as
#'       the counts raster, where cell values are LiDAR capture dates coded as
#'       eight digit integers (yyyymmdd). Cells located within the common edge
#'       of adjacent tiles with different capture dates are assigned the date
#'       of the earlier tile.}
#'   }
#'
#' @export
#'
db_export_stratum_counts <- function(
  dbsettings,
  geom,
  outpath,
  with.purpose = c("general", "postfire"),
  time.interval = NULL,
  default.epsg = NULL,
  overlap.action = c("latest", "earliest", "year", "fail"),
  overlap.yearorder = NULL) {

  with.purpose <- match.arg(with.purpose)
  overlap.action <- match.arg(overlap.action)

  if (overlap.action == "year") {
    if (length(overlap.yearorder) < 1 || !is.numeric(overlap.yearorder))
      stop("overlap.yearorder should be a vector of one or more four digit year numbers")

    overlap.yearorder <- unique(overlap.yearorder)

    if (any(overlap.yearorder < 1990 | overlap.yearorder > 2100))
      stop("overlap.yearorder values expected to be in the range 1990 to 2100")
  }

  if (inherits(geom, what = c("sfc", "sfg"))) {
    g <- geom

  } else if (inherits(geom, what = "sf")) {
    isgeom <- sapply(geom, inherits, what = "sfc")
    if (!sum(isgeom) == 1)
      stop("Failed to find geometry column in sf data frame")
    g <- geom[, isgeom, drop=TRUE]

  } else if (inherits(geom, what = "Extent")) {
    # raster::Extent object
    g <- sf::st_sfc(.extentToPolygon(geom))

    if (is.na(raster::crs(geom))) {
      if (is.null(default.epsg))
        stop("EPSG code must be provided for an Extent object without ",
             "an attached CRS")

      sf::st_crs(g) <- default.epsg
    } else {
      sf::st_crs(g) <- raster::crs(geom)
    }

  } else if (inherits(geom, what = "matrix")) {
    if (is.null(default.epsg))
      stop("EPSG code must be provided for a matrix")

    g <- sf::st_sfc(.extentToPolygon(geom))
    sf::st_crs(g) <- default.epsg
  }

  g.crs <- sf::st_crs(g)
  if (!is.na(g.crs)) {
    epsg <- g.crs$epsg
  } else {
    epsg <- default.epsg
  }

  # Determine schema based on EPSG
  schema.epsgs <- 28300 + as.integer(stringr::str_extract(dbsettings$schema, "\\d{2}$"))
  i <- match(epsg, schema.epsgs)
  if (is.na(i)) {
    stop("No database schema corresponds to feature EPSG code ", epsg)
  }
  schema <- dbsettings$schema[i]


  if (is.null(time.interval)) {
    time.interval.type <- "none"
  } else if (lubridate::is.interval(time.interval)) {
    time.interval.type <- "interval"
  } else if(is.vector(time.interval) &&
          is.numeric(time.interval) &&
          all(stringr::str_detect(time.interval, "^\\d{4}")) ) {
    time.interval.type <- "years"
  } else {
    stop("time.interval must be either a lubridate interval object or ",
         "a vector of four-digit year numbers")
  }


  p <- .get_pool(dbsettings)
  con <- pool::poolCheckout(p)

  g.values <- sapply(g, function(gg) {
    glue::glue("ST_GeomFromText('{sf::st_as_text(gg)}', {epsg})")
  })

  g.values <- paste( paste("(", g.values, ")"), collapse = ",")

  # Load query features into a temp table
  command <- glue::glue("
    CREATE TEMPORARY TABLE tmp_features (
      feature geometry(GEOMETRY, {epsg}) NOT NULL
    );
    --------
    INSERT INTO tmp_features (feature)
      VALUES {g.values};")

  pool::dbExecute(con, command)

  # Identify tiles that intersect with the features
  command <- glue::glue("
    SELECT m.id as meta_id, m.capture_start, m.capture_end FROM
      {schema}.{dbsettings$TABLE_METADATA} AS m, tmp_features as f
      WHERE ST_Intersects(m.bounds, f.feature) AND purpose = '{with.purpose}';")

  tiles <- pool::dbGetQuery(con, command)

  if (nrow(tiles) == 0) {
    pool::dbRollback(con)
    pool::poolReturn(con)
    message("No LAS tiles in the database intersect the query features")
    return(NULL)
  }

  # If a time interval is specified, find the subset of rasters that
  # falls within the interval
  if (time.interval.type != "none") {
    if (time.interval.type == "years") {
      ii <- lubridate::year(tiles$capture_start) %in% time.interval |
        lubridate::year(tiles$capture_end) %in% time.interval
    }
    if (time.interval.type == "interval") {
      ii <- lubridate::`%within%`(tiles$capture_start, time.interval) |
        lubridate::`%within%`(tiles$capture_end, time.interval)
    }

    if (!any(ii)) {
      message("No LAS tiles for the query features are within the time interval")
      pool::dbRollback(con)
      pool::poolReturn(con)
      return(NULL)
    }

    tiles <- tiles[ii, ]
  }

  # Check for rasters with (approximately) the same bounds. This occurs when
  # a 2x2km tile has been flown more than once. For speed, we simply check
  # whether a tile's centroid is overlapped by one or more other tiles within
  # the time period of interest (where defined)
  #
  ids.txt <- paste(tiles$meta_id, collapse = ", ")

  command <- glue::glue("
    SELECT a.id as id1, b.id as id2
      FROM {schema}.{dbsettings$TABLE_METADATA} a, {schema}.{dbsettings$TABLE_METADATA} b
      WHERE a.id IN ({ids.txt}) AND b.id IN ({ids.txt}) AND
        ST_Intersects(ST_Centroid(a.bounds), b.bounds) AND
        a.id < b.id;")

  # Deal with any overlapping tiles (i.e. largely overlapping bounds indicating
  # different scan times for the same bounds)
  #
  overlaps <- pool::dbGetQuery(con, command)

  if (nrow(overlaps) > 0) {
    if (overlap.action == "fail") {
      stop("Export cancelled. Overlapping tiles detected and overlap.action == 'fail'")

    } else {
      id1s <- unique(overlaps$id1)
      n <- length(id1s)
      msg <- ifelse(n == 1, "Resolving 1 set", paste("Resolving", n, "sets"))
      message(msg, " of overlapping tiles")

      chosen.ids <- sapply(id1s, function(id1) {
        ii <- overlaps$id1 == id1
        ids <- c(id1, overlaps$id2[ii])
        times <- tiles$capture_start[ tiles$meta_id %in% ids ]

        if (overlap.action == "latest") {
          k <- which(times == max(times))[1]

        } else if (overlap.action == "earliest") {
          k <- which(times == min(times))[1]

        } else if (overlap.action == "year") {
          years <- lubridate::year(times)
          pos <- match(years, overlap.yearorder)
          if (all(is.na(pos))) { # it seems we don't want any of these tile years
            k <- NA
          } else {
            k <- which(pos == min(pos, na.rm = TRUE))[1]
          }
        }

        if (is.na(k)) {
          NA
        } else {
          ids[k]
        }
      })

      ii <- tiles$meta_id %in% chosen.ids
      tiles <- tiles[ii, ]
    }
  }


  # Finally, do the export
  #
  ids.txt <- paste(tiles$meta_id, collapse = ", ")

  pool::dbBegin(con)
  command <- glue::glue("
    CREATE TEMPORARY TABLE tmp_export_rast AS
      SELECT lo_from_bytea(0, ST_AsGDALRaster(ST_Union(rast, 'SUM'), 'GTiff')) AS loid
      FROM {schema}.{dbsettings$TABLE_COUNTS_LAS}
      WHERE meta_id IN ({ids.txt});
    --------
    SELECT lo_export(loid, '{outpath}')
      FROM tmp_export_rast;
    --------
    SELECT lo_unlink(loid)
      FROM tmp_export_rast;
    --------
    DROP TABLE tmp_features;
    DROP TABLE tmp_export_rast;
  ")

  pool::dbExecute(con, command)
  pool::dbCommit(con)

  if (file.exists(outpath)) {
    rcounts <- raster::stack(outpath)
    names(rcounts) <- CERMBlidar::StrataCERMB$name
  } else {
    pool::poolReturn(con)
    stop("GeoTIFF file for exported point counts could not be created")
  }

  # Create raster of tile scan times
  command <- glue::glue("
    SELECT id as meta_id, capture_start, bounds
      FROM {schema}.{dbsettings$TABLE_METADATA}
      WHERE id IN ({ids.txt});
  ")

  tiles <- sf::st_read(con, query = command)
  tiles$i_time <- as.integer( format(tiles$capture_start, "%Y%m%d") )

  rdates <- fasterize::fasterize(tiles, rcounts, field = "i_time", fun = "min")

  list(counts = rcounts, dates = rdates)
}


#' Summarize number of LAS tiles by map name
#'
#' Gets a summary of the number of LAS tiles stored in the database by
#' map zone (schema), map name and year.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param with.purpose Defines the subset of data to summarize according to
#'   image purpose. One of \code{'general'} (default) for general purpose
#'   imagery; \code{'postfire'} for specially flown, post-fire imagery; or
#'   \code{'all'} for all imagery. May be abbreviated.
#'
#' @return A data frame giving, for each combination of map zone (schema),
#'   map name and year, the number of LAS tiles imported.
#'
#' @export
#'
db_summary <- function(dbsettings, with.purpose = c("general", "postfire", "all")) {
  p <- .get_pool(dbsettings)

  with.purpose <- match.arg(with.purpose)

  where.clause <- switch(
    with.purpose,
    general = "WHERE purpose = 'general'",
    postfire = "WHERE purpose = 'postfire'",
    all = "")

  # Search all schemas
  res <- lapply(dbsettings$schema, function(schema) {
    command <- glue::glue("
      SELECT purpose, zone, mapname, year, count(id) AS ntiles FROM (
        SELECT id, purpose, '{schema}' AS zone, mapname,
          EXTRACT ('year' from capture_start) AS year
        FROM {schema}.{dbsettings$TABLE_METADATA}
        {where.clause}) foo
      GROUP BY (purpose, zone, mapname, year)
      ORDER BY (purpose, zone, mapname, year);")

    pool::dbGetQuery(p, command)
  })

  dplyr::bind_rows(res)
}


#' Submit an arbitrary SQL query
#'
#' This is a short-cut function that retrieves the database connection object from a
#' \code{dbsettings} list, submits an SQL query, and returns the results as a data frame.
#' It is an alternative to composing queries as \code{dplyr} pipelines.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param query SQL query as a character string or similar (e.g. \code{glue}
#'   string).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' Sys.setenv(PGPASSWORD = "secret")
#' DBSettings <- db_connect_postgis("cermb_lidar")
#'
#' # Simple query - count records in one of the tables
#' res <- db_get_query(DBSettings,
#'                     "select count(*) as n from mgazone56.point_counts")
#'
#' # A more complex query: find overlapping LAS tiles in selected map sheets
#' library(glue)
#'
#' mapnames <- c("Wallerawang", "StAlbans", "Gosford",
#'               "MountPomany", "HowesValley", "Cessnock")
#'
#' values <- paste(mapnames, collapse = ", ")
#'
#' # Using spatial self-join on the metadata table
#' #
#' query <- glue("select a.id as id1, b.id as id2, mapname, purpose, capture_start
#'               from mgazone56.las_metadata as a, mgazone56.las_metadata as b,
#'               where mapname in ({values}) and
#'               ST_Intersects(ST_Centroid(a.bounds), b.bounds) and
#'               a.id < b.id;")
#'
#' res <- db_get_query(DBSettings, query)
#' }
#'
db_get_query <- function(dbsettings, query) {
  if (!stringr::str_detect(query, "\\;\\s*$")) query <- paste0(query, ";")
  p <- .get_pool(dbsettings)
  pool::dbGetQuery(p, query)
}


#' Check for any records in metadata tables with no associated point count rasters
#'
#' Each record in a \code{point_counts} table, which contains rasters of point
#' counts for vertical strata, is linked to a parent record in the
#' \code{las_metadata} table within the same schema (e.g. 'mgazone56') via the
#' relation \code{point_counts.meta_id = las_metadata.id}. The database will not
#' allow a point counts record without an associated metadata record, but it
#' will allow a metadata record without any point counts records. Usually, this
#' will indicate that some problem occurred during import. This function checks
#' for any such childless metadata records.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param schema A character vector giving the names of one or more schemas
#'   (e.g. "mgazone56") to check. If \code{NULL} (default), all schemas are
#'   checked.
#'
#' @return A data frame. If childless meta-data records were found, the schema,
#'   integer id and filename will be listed for each. If no such records were
#'   found the data frame will have zero rows and columns.
#'
#' @export
#'
db_check_childless_metadata <- function(dbsettings, schema = NULL) {
  if (is.null(schema)) schema <- dbsettings$schema

  res <- lapply(schema, function(s) {
    cmd <- glue::glue("select '{s}' as schema, id, filename from \\
                      {s}.{dbsettings$TABLE_METADATA} \\
                      where id NOT IN (
                      select meta_id from {s}.{dbsettings$TABLE_COUNTS_LAS})")

    db_get_query(dbsettings, cmd)
  })

  dplyr::bind_rows(res)
}

#' Check if a table exists in the given database
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param tablename Name of the table in the form \code{schema.tablename}.
#'
#' @return \code{TRUE} if the table was found; \code{FALSE} otherwise.
#'
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @examples
#' \dontrun{
#' dbsettings <- db_connect_postgis(...)
#' pg_table_exists(dbsettings, "lidar.point_counts_union")
#' }
#'
#' @export
#'
pg_table_exists <- function(dbsettings, tablename) {
  p <- .get_pool(dbsettings)
  command <- glue::glue("select count(*) as n from {tablename};")

  o <- options(warn = -1, show.error.messages = FALSE)
  x <- try(pool::dbGetQuery(p, command))
  options(o)

  !is.null(x)
}


# Retrieve the database schema that corresponds to an EPSG code
.get_schema_for_epsg <- function(dbsettings, epsg) {
  i <- match(epsg, dbsettings$epsg)
  if (is.na(i)) stop("No database schema supports EPSG code ", epsg)

  dbsettings$schema[i]
}


# Extract filename and extension from a path
.file_from_path <- function(paths) {
  stringr::str_extract(paths, "[\\w\\.\\-]+$")
}

# Remove extension from filename
.file_remove_extension <- function(filenames) {
  ext <- "\\.\\w+$"

  out <- sapply(filenames, function(fname) {
    if (stringr::str_detect(fname, ext)) {
      pos <- stringr::str_locate(fname, ext)[1,1] - 1
    } else {
      pos <- -1
    }
    stringr::str_sub(fname, 1, pos)
  })

  unname(out)
}

# Format a time stamp to include the time zone
# (used by db_load_tile_metadata)
.tformat <- function(timestamp, tz = "UTC") {
  if (inherits(timestamp, c("POSIXt", "Date"))) {
    x <- timestamp
  } else if (inherits(timestamp, "character")) {
    x <- lubridate::parse_date_time(timestamp,
                                    orders = c("ymd", "ymd H", "ymd HM", "ymd HMS"),
                                    tz=tz)
  } else {
    stop("Argument timestamp should be POSIXt, Date, character")
  }

  format(timestamp, format = "%Y-%m-%d %H:%M:%S", usetz = TRUE)
}


# Put single quotes around strings
.single_quote <- function(x) {
  if (length(x) > 0) x <- paste0("'", x, "'")
  x
}


# Get first elment from a vector
.first_elem <- function(x, msg = TRUE) {
  if (is.null(x)) {
    NULL
  }
  else if (length(x) > 1) {
    if (msg) {
      nm <- deparse(substitute(x))
      message("Ignoring all but first element for ", nm)
    }
    x[1]
  } else {
    x
  }
}


.extentToPolygon <- function(x) {
  stopifnot(inherits(x, c("matrix", "Extent")))

  if (class(x) == "matrix") {
    xy <- c(x[1,], x[2,])
  } else {
    xy <- c(x@xmin, x@xmax, x@ymin, x@ymax)
  }

  ii <- c(1,3, 1,4, 2,4, 2,3, 1,3)

  pts <- matrix(xy[ii], ncol=2, byrow = TRUE)
  sf::st_polygon(list(pts))
}

