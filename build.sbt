val casbahVersion = "2.8.1"

name := "ems-export"

val mongo = Seq(
  "org.mongodb" %% "casbah-core"    % casbahVersion,
  "org.mongodb" %% "casbah-gridfs"  % casbahVersion,
  "org.mongodb" %% "casbah-query"   % casbahVersion,
  "joda-time"   %  "joda-time"      % "2.8.2",
  "io.argonaut" %% "argonaut"       % "6.1"
)


libraryDependencies ++= mongo
