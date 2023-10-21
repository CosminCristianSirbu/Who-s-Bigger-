package rhul.ac.uk;

import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/**
 * Testing Suite for the Integration Tests.
 */
@SelectPackages({"rhul.ac.uk.integrationtests"})
@Suite
public class IntegrationTestsSuite {
}
