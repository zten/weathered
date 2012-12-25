require(['components/jquery/jquery', 'components/d3/d3', 'lib/topojson'], function () {
  // Cribbed from the canvas demo of d3 v3
  (function () {
    var land;
    var borders;
    var stations;

    var width = 960;
    var height = 800;

    var minScale = 200;
    var maxScale = 6400;
    var scale = 400;

    var projection = d3.geo.orthographic()
        .scale(scale)
        .clipAngle(90)
        .translate([width / 2, height / 2]);

    var canvas = d3.select("body").append("canvas")
        .attr("id", "globe")
        .attr("width", width)
        .attr("height", height);

    var c = canvas.node().getContext("2d");

    var path = d3.geo.path()
        .projection(projection)
        .context(c);

    var λ = d3.scale.linear()
      .domain([0, width])
      .range([-180, 180]);

    var φ = d3.scale.linear()
      .domain([0, height])
      .range([90, -90]);

    function redraw() {
      c.clearRect(0, 0, width, height);
      if (land != null) {
        c.fillStyle = "#bbb", c.beginPath(), path(land), c.fill();      
      }
      if (borders != null) {
        c.strokeStyle = "#fff", c.lineWidth = .5, c.beginPath(), path(borders), c.stroke();          
      }
      if (land != null && borders != null && stations != null) {
        c.strokeStyle = "#3A84A8", c.lineWidth = 1, c.beginPath(), path(stations), c.stroke();          
      }
    }
    
    var mouseDown = false;

    canvas.on("mousemove", function () {
      if (mouseDown) {
        var p = d3.mouse(this);
        projection.rotate([λ(p[0]), φ(p[1])]);
        redraw();         
      }
    }).on("mousedown", function () {
      mouseDown = true;
    }).on("mouseup", function () {
      mouseDown = false;
    });

    function drawAt(longitude, lat) {
      projection.rotate([-longitude, -lat]);
      redraw();
    }

    d3.json("/data/world-110m.json", function (error, world) {
      land = topojson.object(world, world.objects.land);
      borders = topojson.mesh(world, world.objects.countries, function(a, b) { return a.id !== b.id; });

      //drawAt(-71, 42);
    });

    d3.json("/stations", function (error, stationData) {
      stations = stationData;
      redraw();
    });

    var globe = document.getElementById("globe");

    globe.onmousewheel = function (event) {
      // http://stackoverflow.com/questions/2916081/zoom-in-on-a-point-using-scale-and-translate
      var wheel = event.wheelDelta/120;

      var zoom = Math.pow(1 + Math.abs(wheel)/2 , wheel > 0 ? 1 : -1);

      var initScale = scale;
      initScale *= zoom;
      if (initScale <= maxScale && initScale >= minScale) {
        scale = initScale;
      }

      projection.scale(scale);
      redraw();
    };
  })();

// d3.json("data/world-countries.json", function(collection) {
//   feature = svg.selectAll("path")
//       .data(collection.features)
//     .enter().append("svg:path")
//       .attr("d", path);

//   feature.append("svg:title")
//       .text(function(d) { return d.properties.name; });
//   }
// );

  // now that we've loaded the countries, we can load the station data
  // this quirkiness is due to svg dom element order determining the equivalent
  // of the z-index.

  //var circles = svg.append("svg:g").attr("id", "circles");
//   $.ajax({
// 		url: "/stations",
// 		dataType: "json"
// 	}).done(function (data) {
// 		circleFeature = circles.selectAll("circle").data(data.features).enter()
// 			.append("svg:path")
// 			.attr("transform", circleTransformer)
// 			.attr("d", path);

// 		circleFeature.append("svg:title").text(function (d) { return d.properties.name; });
// 	});
// });


});
