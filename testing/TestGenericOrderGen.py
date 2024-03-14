import unittest
from datetime import  date
from controller import orderGen
import inspect
from testing import TestOrderGenHelper
from config.cache.BasicCache import BasicCache
unittest.TestLoader.sortTestMethodsUsing = None

# You can call Inidvidual Test Case as well, But you have to set Environment Variables in Configuration First
# DEFAULT GLOBAL MOCK_PROPERTIES, ANY CHANGE(ADDITION/REMOVAL) SHOULD BE DONE IN setUp method as well

def setDefaultMockProperties():
    BasicCache().set('SCHEMA_NAME', 'BRAHMA_V1_DEV_AG')
    BasicCache().set('NET_DELTA', 1.0)
    BasicCache().set('isCallAdjustmentSquareOffDone', False)
    BasicCache().set('isPutAdjustmentSquareOffDone', False)
    BasicCache().set('CURRENT_PRICE_CALL', 100)
    BasicCache().set('CURRENT_PRICE_PUT', 100)
    BasicCache().set('NET_PNL_OVERALL', 1000)
    BasicCache().set('NET_PNL_OVERALL_PORTFOLIO_PNL_TGT_SL_PCT', 1000)
    BasicCache().set('EXIT_TIME_CURRENT', '3:00')
    BasicCache().set('CURRENT_DATE', date(2022, 1, 1))
    BasicCache().set('LAST_DATE_BEFORE_EXPIRY', date.today())
    BasicCache().set('TWO_DAYS_BEFORE_EXPIRY', date(2022, 1, 2))

class TestGenericOrderGen(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("Inside setUpClass!!")
        pass

    @classmethod
    def tearDownClass(cls):
        cls.MOCK_PROPERTIES = {}

    @classmethod
    def setUp(cls):
        from config.logging.LogConfig import LogConfig
        LogConfig()
        setDefaultMockProperties()

        print("Inside setUp!!")
        # THIS METHOD IS CALLED BEFORE EVERY TEST CASE
        # EVERY TEST CASE IS INDEPENDENT TEST CASE
        # CLEANUP/DEFAULT MOCK_PROPERTIES BEFORE EVERY TEST CASE
        TestOrderGenHelper.cleanUp(BasicCache().get('SCHEMA_NAME'))
        pass

    def test11_initiate_positions(self):
        # You can call this Inidvidual Test Case, But you have to set Environment Variables in Configuration First
        # Check Edit Configuration for Examples
        print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
        print("-------------------------------------------------------------------------------------------------------")
        orderGen.placeOrders()
        df_positions_tracking_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingLatestData(BasicCache().get('SCHEMA_NAME'))
        self.assertEquals(df_positions_tracking_last_itr_result['NET_PNL_OVERALL'].iloc[0], -22.50)

    def test12_call_adjustment(self):
        setDefaultMockProperties()
        print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
        print("-------------------------------------------------------------------------------------------------------")

        # UPDATE MOCK PROPERTIES
        BasicCache().set('CURRENT_PRICE_CALL', 150)
        orderGen.placeOrders()

        # UPDATE MOCK PROPERTIES
        BasicCache().set('isCallAdjustmentSquareOffDone', True)
        orderGen.placeOrders()

        # SQUAREOFF
        BasicCache().set('EXIT_TIME_CURRENT', '15:20')
        BasicCache().set('CURRENT_DATE', date.today())
        orderGen.placeOrders()

        # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
        df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData(BasicCache().get('SCHEMA_NAME'))

        # ASSERTIONS
        self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -22.50)
        self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
                            str.contains("_SQUAREOFF_FINAL")]) == 1)

    def test13_put_adjustment(self):
        setDefaultMockProperties()
        print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
        print("-------------------------------------------------------------------------------------------------------")

        # UPDATE MOCK PROPERTIES
        BasicCache().set('CURRENT_PRICE_PUT', 150)
        orderGen.placeOrders()

        # UPDATE MOCK PROPERTIES
        BasicCache().set('isPutAdjustmentSquareOffDone', True)
        orderGen.placeOrders()

        # SQUAREOFF
        BasicCache().set('EXIT_TIME_CURRENT', '15:20')
        BasicCache().set('CURRENT_DATE', date.today())
        orderGen.placeOrders()

        # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
        df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData(
            BasicCache().get('SCHEMA_NAME'))

        # ASSERTIONS
        self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -22.50)
        self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
                str.contains("_SQUAREOFF_FINAL")]) == 1)

    def test14_call_put_adjustment_profit(self):
        setDefaultMockProperties()
        print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
        print("-------------------------------------------------------------------------------------------------------")

        # UPDATE MOCK PROPERTIES
        BasicCache().set('CURRENT_PRICE_CALL', 150)
        orderGen.placeOrders()

        # UPDATE MOCK PROPERTIES
        BasicCache().set('isCallAdjustmentSquareOffDone', True)
        orderGen.placeOrders()

        # UPDATE MOCK PROPERTIES
        BasicCache().set('CURRENT_PRICE_PUT', 80)
        orderGen.placeOrders()

        # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
        df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData(BasicCache().get('SCHEMA_NAME'))

        # ASSERTIONS
        self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -22.50)
        self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
                str.contains("_SQUAREOFF")]) == 1)

    def test15_call_put_adjustment_loss(self):
        setDefaultMockProperties()
        print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
        print("-------------------------------------------------------------------------------------------------------")

        # UPDATE MOCK PROPERTIES
        BasicCache().set('CURRENT_PRICE_CALL', 150)
        orderGen.placeOrders()

        # UPDATE MOCK PROPERTIES
        BasicCache().set('isCallAdjustmentSquareOffDone', True)
        orderGen.placeOrders()

        # UPDATE MOCK PROPERTIES
        BasicCache().set('CURRENT_PRICE_PUT', 150)
        orderGen.placeOrders()

        # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
        df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData(BasicCache().get('SCHEMA_NAME'))

        # ASSERTIONS
        self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -22.50)
        self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
                str.contains("_SQUAREOFF")]) == 1)


    # def test14_stoploss_squareoff(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['NET_PNL_OVERALL'] = -21000
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -977.50)
    #     self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #                         str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    # def test15_target_squareoff(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['NET_PNL_OVERALL'] = 25000
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -977.50)
    #     self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #                         str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    # def test16_exit_positions_time(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['EXIT_TIME_CURRENT'] = '14:30'
    #     MOCK_PROPERTIES['CURRENT_DATE'] = date.today()
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -977.50)
    #     self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #                         str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    # def test17_exit_positions_last_day_before_expiry_range_1(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['NET_PNL_OVERALL'] = 18000
    #     MOCK_PROPERTIES['EXIT_TIME_CURRENT'] = '10:00'
    #     MOCK_PROPERTIES['CURRENT_DATE'] = date.today()
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -977.50)
    #     self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #                         str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    # def test18_exit_positions_last_day_before_expiry_range_2(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['NET_PNL_OVERALL'] = 13500
    #     MOCK_PROPERTIES['EXIT_TIME_CURRENT'] = '12:00'
    #     MOCK_PROPERTIES['CURRENT_DATE'] = date.today()
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -977.50)
    #     self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #                         str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    # def test19_exit_positions_last_day_before_expiry_range_3(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['NET_PNL_OVERALL'] = 10000
    #     MOCK_PROPERTIES['EXIT_TIME_CURRENT'] = '13:15'
    #     MOCK_PROPERTIES['CURRENT_DATE'] = date.today()
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -977.50)
    #     self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #                         str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    #
    # def test20_exit_positions_last_hour_by_slashing_target(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['NET_PNL_OVERALL'] = 20000
    #     MOCK_PROPERTIES['EXIT_TIME_CURRENT'] = '15:00'
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -977.50)
    #     self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #                         str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    # def test21_exit_positions_two_days_before_expiry_range_1(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['NET_PNL_OVERALL'] = 18000
    #     MOCK_PROPERTIES['EXIT_TIME_CURRENT'] = '10:00'
    #     MOCK_PROPERTIES['CURRENT_DATE'] = date.today()
    #     MOCK_PROPERTIES['TWO_DAYS_BEFORE_EXPIRY'] = date.today()
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -977.50)
    #     self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #                         str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    # def test22_portfolio_tgt_profit_pct_squareoff(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['NET_PNL_OVERALL_PORTFOLIO_PNL_TGT_SL_PCT'] = 100000
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -977.50)
    #     self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #                         str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    # def test23_portfolio_tgt_stoploss_pct_squareoff(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['NET_PNL_OVERALL_PORTFOLIO_PNL_TGT_SL_PCT'] = -100000
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -977.50)
    #     self.assertTrue(len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #                         str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    #
    # def test25_call_adjustment_sqaureoff(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     MOCK_PROPERTIES['CURRENT_PRICE_CALL_LEG_3'] = 200
    #     OrderGenerationMain.placeOrders()
    #
    #     MOCK_PROPERTIES['isCallAdjustmentDone'] = True
    #     MOCK_PROPERTIES['CURRENT_PRICE_CALL_LEG_3'] = 123
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['isCallAdjustmentDone'] = False
    #
    #     MOCK_PROPERTIES['CURRENT_PRICE_CALL_LEG_3'] = 160
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['CURRENT_PRICE_CALL_LEG_3'] = 201
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['isCallAdjustmentDone'] = True
    #
    #     MOCK_PROPERTIES['CURRENT_PRICE_CALL_LEG_3'] = 123
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['isCallAdjustmentDone'] = False
    #
    #     MOCK_PROPERTIES['CURRENT_PRICE_CALL_LEG_3'] = 160
    #     OrderGenerationMain.placeOrders()
    #
    #     # SQUAREOFF
    #     MOCK_PROPERTIES['EXIT_TIME_CURRENT'] = '14:30'
    #     MOCK_PROPERTIES['CURRENT_DATE'] = date.today()
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -1212.5)
    #     self.assertTrue(
    #         len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #             str.contains("_SQUAREOFF_FINAL")]) >= 8)
    #
    # def test26_put_adjustment_sqaureoff(self):
    #     print("\nRunning TestCase ::: {}".format(inspect.stack()[0][3]))
    #     print("-------------------------------------------------------------------------------------------------------")
    #     MOCK_PROPERTIES['CURRENT_PRICE_PUT_LEG_3'] = 200
    #     OrderGenerationMain.placeOrders()
    #
    #     MOCK_PROPERTIES['isPutAdjustmentDone'] = True
    #     MOCK_PROPERTIES['CURRENT_PRICE_PUT_LEG_3'] = 123
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['isPutAdjustmentDone'] = False
    #
    #     MOCK_PROPERTIES['CURRENT_PRICE_PUT_LEG_3'] = 160
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['CURRENT_PRICE_PUT_LEG_3'] = 201
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['isPutAdjustmentDone'] = True
    #
    #     MOCK_PROPERTIES['CURRENT_PRICE_PUT_LEG_3'] = 123
    #     OrderGenerationMain.placeOrders()
    #     MOCK_PROPERTIES['isPutAdjustmentDone'] = False
    #
    #     MOCK_PROPERTIES['CURRENT_PRICE_PUT_LEG_3'] = 160
    #     OrderGenerationMain.placeOrders()
    #
    #     # SQUAREOFF
    #     MOCK_PROPERTIES['EXIT_TIME_CURRENT'] = '14:30'
    #     MOCK_PROPERTIES['CURRENT_DATE'] = date.today()
    #     OrderGenerationMain.placeOrders()
    #
    #     # FETCH POSITIONS TRACKING LAST BACK UP ITERATION DATA
    #     df_positions_tracking_bak_last_itr_result = TestOrderGenHelper.fetchPositionsTrackingBackUpLatestData()
    #
    #     # ASSERTIONS
    #     self.assertEquals(df_positions_tracking_bak_last_itr_result['NET_PNL_OVERALL'].iloc[0], -1197.5)
    #     self.assertTrue(
    #         len(df_positions_tracking_bak_last_itr_result[df_positions_tracking_bak_last_itr_result["ORDER_MANIFEST"].
    #             str.contains("_SQUAREOFF_FINAL")]) >= 8)


if __name__ == '__main__':
    unittest.main()