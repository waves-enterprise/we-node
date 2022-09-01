Seq(
  "com.eed3si9n"        % "sbt-assembly"             % "1.2.0",
  "se.marcuslonnberg"   % "sbt-docker"               % "1.8.2",
  "com.typesafe.sbt"    % "sbt-git"                  % "1.0.0",
  "org.scala-js"        % "sbt-scalajs"              % "0.6.29",
  "org.portable-scala"  % "sbt-crossproject"         % "0.6.1",
  "org.portable-scala"  % "sbt-scalajs-crossproject" % "0.6.1",
  "org.scalameta"       % "sbt-scalafmt"             % "2.4.6",
  "com.github.tkawachi" % "sbt-repeat"               % "0.1.0"
).map(addSbtPlugin)
