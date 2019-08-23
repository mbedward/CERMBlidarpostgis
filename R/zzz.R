.onLoad <- function(libname, pkgname) {
  assign("._cermblidar_settings", list(), pos = 1)
}

.onUnload <- function(libpath) {
  if (exists(._cermblidar_settings, where = 1)) rm(._cermblidar_settings, pos = 1)
}
