# Archives Unleashed Toolkit (AUT):
# An open-source platform for analyzing web archives.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql.column import Column
from pyspark.sql.column import _to_java_column
from pyspark.sql.column import _to_seq
from pyspark.sql.functions import col

class ScalaUDF:
    """
    ScalaUDF is a class to be instantiated when you need to use a Scala UDF
    in a PySpark job. Calling add(UDFName) will create a method of the
    class instance that corresponds to the UDF in question, if it exists.

    Note that Scala UDFs must first implement the getUDF() method before they
    can be imported in this way. A sample implementation would be:

    def getUDF: UserDefinedFunction = udf((text: String) => apply(text))
    """

    def add(self, sc, udf_name):
        def new_udf(col_names):
            udf_pkg = sc._jvm.io.archivesunleashed.spark.matchbox
            udf = getattr(udf_pkg, udf_name).getUDF()
            # For each input column in col_names, create a seq for the UDF
            seq_params = (_to_seq(sc, [col], _to_java_column) for col in col_names)
            # Unpack seq parameters and apply UDF
            return Column(udf.apply(*seq_params))
        new_udf.__doc__ = "{} is a Scala UDF imported for PySpark".format(udf_name)
        new_udf.__name__ = udf_name
        setattr(self, new_udf.__name__, new_udf)
