service "cdcron" {
    policy = "write"
}

session "cdcron" {
    policy = "write"
}

key "service/cdcron/leader" {
    policy = "write"
}
