#' Get the bounding envelope of a raster as a polygon
#'
#' This returns the bounds of a raster as an \code{sf} polygon object,
#' suitable for writing to PostGIS.
#'
#' @param r The raster.
#'
#' @return Raster bounds as an \code{sf::polygon} object.
#'
#' @export
#'
get_raster_bounds <- function(r) {
  xys <- c(xmin(r), ymin(r), xmax(r), ymax(r))
  verts <- matrix(xys[c(1,2, 1,4, 3,4, 3,2, 1,2)], ncol = 2, byrow = TRUE)
  sf::st_polygon(list(verts))
}
