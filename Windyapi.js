const options = {
    // Required: API key
    key: 'rlwCQaZvqojZLyHZIgcHIusnL6Wu6cBa', // REPLACE WITH YOUR KEY !!!

    // Put additional console output
    verbose: true,

    // Optional: Initial state of the map
    lat: -29,
    lon: 134.45,
    zoom: 4,
};

// Initialize Windy API
windyInit(options, windyAPI => {
    // windyAPI is ready, and contain 'map', 'store',
    // 'picker' and other usefull stuff

    const { map } = windyAPI;
    // .map is instance of Leaflet map


});
