package main

type Stop struct {
	ParentStation string `json:"parent_station"`
	Location      struct {
		Lat float64 `json:"lat"`
		Lon float64 `json:"lon"`
	} `json:"location"`
	StopName string `json:"stop_name"`
}

type StopTimeUpdate struct {
	Arrival *struct {
		Time  int64 `json:"time,string"`
		Delay int   `json:"delay,omitempty"`
	} `json:"arrival,omitempty"`
	Departure *struct {
		Time  int64 `json:"time,string"`
		Delay int   `json:"delay,omitempty"`
	} `json:"departure,omitempty"`
	ScheduleRelationship string `json:"scheduleRelationship"`
	StopID               string `json:"stopId"`
	StopSequence         int    `json:"stopSequence"`
}

type DestStopTimeUpdate struct {
	StopTimeUpdate
	Stop          Stop   `json:"stop,omitempty"`
	TripId        string `json:"tripId"`
	RouteId       string `json:"routeId"`
	StartTime     string `json:"startTime"`
	StartDate     string `json:"startDate"`
	StartDatetime string `json:"startDatetime"`
}

type SourceRecord struct {
	Header struct {
		FeedVersion         string `json:"feedVersion"`
		GtfsRealtimeVersion string `json:"gtfsRealtimeVersion"`
		Incrementality      string `json:"incrementality"`
		Timestamp           int64  `json:"timestamp,string"`
	} `json:"header"`
	ID         string `json:"id"`
	TripUpdate struct {
		StopTimeUpdate []StopTimeUpdate `json:"stopTimeUpdate"`
		Trip           struct {
			RouteID              string `json:"routeId"`
			ScheduleRelationship string `json:"scheduleRelationship"`
			StartDate            string `json:"startDate"`
			StartTime            string `json:"startTime"`
			TripID               string `json:"tripId"`
		} `json:"trip"`
	} `json:"tripUpdate"`
}
