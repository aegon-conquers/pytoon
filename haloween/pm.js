pm.test("Check version is expected value", function () {
    // Get the response as text
    var responseBody = pm.response.text();
    
    // Use a regular expression to extract the version value
    var versionMatch = responseBody.match(/version:\s*['"]?([^'"]+)['"]?/);
    
    // Check if the version was found and matches the expected version
    pm.expect(versionMatch).to.not.be.null; // Ensure there's a match
    pm.expect(versionMatch[1]).to.eql(pm.environment.get("expected_version"));
});
