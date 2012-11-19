require(['components/jquery/jquery', 'components/d3/d3.v2'], function () {

// Cribbed from http://mbostock.github.com/d3/talk/20111018/azimuthal.html

var feature;

var circleFeature;

var chosenScale = 1600;

var projection = d3.geo.azimuthal()
    .scale(chosenScale)
    .origin([-71, 42]) // Boston!
    .mode("orthographic")
    .translate([320, 400]);

var circle = d3.geo.greatCircle()
    .origin(projection.origin());

// TODO fix d3.geo.azimuthal to be consistent with scale
var scale = {
  orthographic: chosenScale,
  stereographic: chosenScale,
  gnomonic: chosenScale,
  equidistant: chosenScale / Math.PI * 2,
  equalarea: chosenScale / Math.SQRT2
};

var path = d3.geo.path()
    .projection(projection);

var svg = d3.select("#body").append("svg:svg")
    .attr("width", 1280)
    .attr("height", 800)
    .on("mousedown", mousedown);

d3.json("data/world-countries.json", function(collection) {
  feature = svg.selectAll("path")
      .data(collection.features)
    .enter().append("svg:path")
      .attr("d", clip);

  feature.append("svg:title")
      .text(function(d) { return d.properties.name; });

  // now that we've loaded the countries, we can load the station data
  // this quirkiness is due to svg dom element order determining the equivalent
  // of the z-index.

  var circles = svg.append("svg:g").attr("id", "circles");
  $.ajax({
		url: "/stations",
		dataType: "json"
	}).done(function (data) {
		circleFeature = circles.selectAll("circle").data(data.features).enter()
			.append("svg:path")
			.attr("transform", circleTransformer)
			.attr("d", circleClip);

		circleFeature.append("svg:title").text(function (d) { return d.properties.name; });
	});
});

d3.select(window)
    .on("mousemove", mousemove)
    .on("mouseup", mouseup);

d3.select("select").on("change", function() {
  projection.mode(this.value).scale(scale[this.value]);
  refresh(750);
});

function circleTransformer(d) {
	return "translate(" + projection(d.geometry.coordinates)[0] + ", " + projection(d.geometry.coordinates)[1] + ")";	
}

var m0,
    o0;

function mousedown() {
  m0 = [d3.event.pageX, d3.event.pageY];
  o0 = projection.origin();
  d3.event.preventDefault();
}

function mousemove() {
  if (m0) {
    var m1 = [d3.event.pageX, d3.event.pageY],
        o1 = [o0[0] + (m0[0] - m1[0]) / 8, o0[1] + (m1[1] - m0[1]) / 8];
    projection.origin(o1);
    circle.origin(o1)
    refresh();
  }
}

function mouseup() {
  if (m0) {
    mousemove();
    m0 = null;
  }
}

function refresh(duration) {
  (duration ? feature.transition().duration(duration) : feature).attr("d", clip);

  (duration ? circleFeature.transition().duration(duration) : circleFeature).attr("transform", circleTransformer).attr("d", circleClip);
}

function clip(d) {
	return path(circle.clip(d));
}

var weatherStationCircle = d3.svg.symbol().size(16)();

function circleClip(d) {
	if (circle.clip(d)) {
		return weatherStationCircle;
	} else {
		return "";
	}
}

});