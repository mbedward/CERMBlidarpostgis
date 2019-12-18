#' New South Wales 100k map sheets
#'
#' An \code{sf} spatial data frame with the number, name and bounds (latitude
#' and longitude) of 1:100,000 topographic maps for New South Wales.
#'
#' @format A spatial data frame with 414 rows and 3 columns:
#' \describe{
#'   \item{mapnumber}{Four digit integer map number.}
#'   \item{mapname}{Map title as used by New South Wales Spatial Services.}
#'   \item{geometry}{Map bounds as an sf::polygon.}
#' }
"nswmaps100k"
