resolvers ++= Seq(
  "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
  "Artima Maven Repository" at "https://repo.artima.com/releases",
  "JBoss" at "https://repository.jboss.org",
  Resolver.sbtPluginRepo("releases")
)

Seq(
  "com.eed3si9n"            % "sbt-assembly"             % "0.14.10",
  "com.typesafe.sbt"        % "sbt-native-packager"      % "1.3.25",
  "org.scalastyle"          %% "scalastyle-sbt-plugin"   % "1.0.0",
  "se.marcuslonnberg"       % "sbt-docker"               % "1.8.2",
  "com.typesafe.sbt"        % "sbt-git"                  % "1.0.0",
  "org.scala-js"            % "sbt-scalajs"              % "0.6.29",
  "org.portable-scala"      % "sbt-crossproject"         % "0.6.1",
  "org.portable-scala"      % "sbt-scalajs-crossproject" % "0.6.1",
  "com.lucidchart"          % "sbt-scalafmt"             % "1.16",
  "com.github.tkawachi"     % "sbt-repeat"               % "0.1.0",
  "com.lightbend.sbt"       % "sbt-proguard"             % "0.3.0",
  "com.codacy"              % "sbt-codacy-coverage"      % "3.0.3",
).map(addSbtPlugin)
