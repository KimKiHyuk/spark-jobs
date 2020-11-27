using System.Collections.Generic;
using System;

namespace emrApp.Model 
{
    [Serializable]
    internal class RegionModel {
        
        private IDictionary<string, string> convTable; 
        
        public IDictionary<string, string> ConversionTable {
            get { return this.convTable; }
        }

        public RegionModel()
        {
            this.convTable = new Dictionary<string, string>()
            {
                {"0", "Orange"},
                {"1", "Liverpool"},
                {"2", "Brisbane"},
                {"3", "Gold Coast"},
                {"4", "Sunshine Coast"},  
                {"5", "Blue Mountains"},
                {"6", "Newcastle"},
                {"7", "Sydney"},
                {"8", "Griffith"},
                {"9", "Darwin"},
            };
        }
    }
}