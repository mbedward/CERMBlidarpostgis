#' Check for a local PostgreSQL/PostGIS installation
#'
#' Checks that PostgreSQL version 11 and the PostGIS extension are installed on
#' the local system and available from the command line.
#'
#' @param quiet If \code{TRUE}, warning messages about missing components are
#'   not printed to the console. The default is \code{FALSE}.
#'
#' @return \code{TRUE} if PostgreSQL/PostGIS is found;
#'   \code{FALSE} otherwise.
#'
#' @export
#'
check_postgis <- function(quiet = FALSE) {
  PGPATH <- "c:/Program Files/PostgreSQL/11/bin"
  if (!dir.exists(PGPATH)) {
    if (!quiet)
      warning("Cannot find path ", PGPATH)
      return(FALSE)
  }

  # Locate required helper applications
  exes <- dir(PGPATH, pattern = "exe$", full.names = TRUE)
  helpers <- c("psql.exe", "raster2pgsql.exe")
  ii <- sapply(helpers, grep, exes)
  ok <- sapply(ii, length) > 0

  if (any(!ok)) {
    if (!quiet)
      warning("Cannot find required database helper applications\n", helpers[!ok])
    return(FALSE)
  }

  .set_runtime_setting("PSQL", exes[ ii["psql.exe"] ])
  .set_runtime_setting("R2P", exes[ ii["raster2pgsql.exe"] ])
  TRUE
}


.check_runtime_setting <- function(setting, desired.value) {
  if (!exists("._cermb_settings", where = 1)) {
    assign("._cermb_settings", list(), pos = 1)
    FALSE
  }

  S <- get("._cermb_settings", pos = 1)

  if (!(setting %in% names(S))) {
    FALSE
  } else {
    S[[setting]] == desired.value
  }
}

.set_runtime_setting <- function(setting, value) {
  if (!exists("._cermb_settings", where = 1)) {
    assign("._cermb_settings", list(), pos = 1)
  }

  S <- get("._cermb_settings", pos = 1)
  S[[setting]] <- value
  assign("._cermb_settings", S, pos = 1)
}

.get_runtime_setting <- function(setting) {
  if (!exists("._cermb_settings", where = 1)) {
    assign("._cermb_settings", list(), pos = 1)
    NULL
  }

  S <- get("._cermb_settings", pos = 1)
  S[[setting]]
}
