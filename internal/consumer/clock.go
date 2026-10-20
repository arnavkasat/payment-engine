package consumer

import "time"

// nowFunc is a variable so tests can freeze time without threading a clock
// through every call site.
var nowFunc = time.Now
