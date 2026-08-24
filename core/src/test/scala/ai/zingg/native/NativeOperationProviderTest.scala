package ai.zingg.native

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class NativeOperationProviderTest {
  @Test def operationInventoryUsesStableIDs(): Unit = {
    assertEquals("similarity.exact", NativeOperation.resolve("similarity.exact").id)
    assertEquals("preprocess.case_normalize", NativeOperation.resolve("preprocess.case_normalize").id)
  }
}
