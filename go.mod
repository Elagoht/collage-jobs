// A collage plugin that runs scheduled jobs and typed background queues, only
// while the application serves, and finishes queued work on shutdown.
//
// It requires collage the way any consumer does.
module github.com/Elagoht/collage-jobs

go 1.26

require github.com/Elagoht/collage v0.54.0

replace github.com/Elagoht/collage => ../collage-jobs-core
