# -*- coding: utf-8 -*-
import arcpy, os, sys

arcpy.env.overwriteOutput = True

class Toolbox(object):
    def __init__(self):
        """The UpdateDataSource Python toolbox includes two tools that allow users to update data sources for layers
        within MXD files that are sourced from enterprise geodatabases using SDE connection files."""
        self.label = "Update MXD Data Source Tools"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [UpdateDataSourcePerMXD, UpdateDataSourcePerDirectory]


class UpdateDataSourcePerMXD(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Update SDE data source in a single MXD"
        self.description = "This tool will find all layers within a single MXD file that use the 'old' geodatabase as " \
                           "a data source, and then change the layer's data source to the 'new' geodatabase."
        self.canRunInBackground = True

    def getParameterInfo(self):
        """Define parameter definitions"""
        param0 = arcpy.Parameter(
            displayName="MXD to be updated",
            name="in_mxd",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        param0.filter.list = ["mxd"]

        param1 = arcpy.Parameter(
            displayName="CSV file with old and new feature classes crosswalked",
            name="xwalk",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        param1.filter.list = ["csv", "txt"]

        param2 = arcpy.Parameter(
            displayName="SDE connection for new data source",
            name="sde_conn",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        param2.filter.list = ["sde"]

        param3 = arcpy.Parameter(
            displayName="File path and name where updated MXD will be saved",
            name="out_mxd",
            datatype="DEFile",
            parameterType="Required",
            direction="Output")
        param3.filter.list = ["mxd"]

        params = [param0, param1, param2, param3]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed. This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool. Imported from a separate Python module."""
        update_data_source(input_mxd=parameters[0].valueAsText,
                           xwalk_tbl=parameters[1].valueAsText,
                           new_src=parameters[2].valueAsText,
                           output_mxd=parameters[3].valueAsText)
        return


class UpdateDataSourcePerDirectory(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Update SDE data source for all MXDs in a directory"
        self.description = "This tool will compile a list of MXDs in a given directory, and then iterate through " \
                           "that list, and within each MXD file, determine layers that use the 'old' geodatabase as " \
                           "a data source, and then change the layer's data source to the 'new' geodatabase. Updated" \
                           "MXDs are saved to the same directory, but with a user-supplied suffix added to the MXD file" \
                           "name."
        self.canRunInBackground = True

    def getParameterInfo(self):
        """Define parameter definitions"""
        param0 = arcpy.Parameter(
            displayName="Directory to search for MXD files to update",
            name="in_folder",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")

        param1 = arcpy.Parameter(
            displayName="CSV file with old and new feature classes crosswalked",
            name="xwalk_tbl",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        param1.filter.list = ["csv", "txt"]

        param2 = arcpy.Parameter(
            displayName="SDE connection for new data source",
            name="sde_conn_new",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input")
        # param2.filter.list = ["Remote Database"]

        param3 = arcpy.Parameter(
            displayName="Suffix added to the end of all updated MXD file names",
            name="suffix",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        params = [param0, param1, param2, param3]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed. This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool. Imported from a separate Python module."""
        arcpy.env.workspace = parameters[0].valueAsText
        mxd_list = compile_mxd_list(parameters[0].valueAsText)
        arcpy.AddMessage('{0} MXD files found in {1}'.format(len(mxd_list), parameters[0].valueAsText))
        xwalk_tbl = parameters[1].valueAsText,
        new_src = parameters[2].valueAsText,
        suffix = parameters[3].valueAsText
        try:
            for mxd in mxd_list:
                output_mxd = '{0}_{1}.mxd'.format(mxd.split('\\')[-1], suffix)
                update_data_source(input_mxd=mxd,
                                   xwalk_tbl=xwalk_tbl,
                                   new_src=new_src,
                                   output_mxd=output_mxd)
        except Exception, e:
            tb = sys.exc_info()[2]
            arcpy.AddError("Line %s" % tb.tb_lineno)
            arcpy.AddError(e)
            arcpy.AddError('There was a problem with the update process...')
        return


def compile_mxd_list(input_dir):
    """
    """
    mxd_list = []
    arcpy.AddMessage('Finding MXD files in {0}...'.format(input_dir))
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith('mxd'.lower()):
                mxd_list.append(os.path.join(root, file))
                arcpy.AddMessage('--{0}'.format(os.path.join(root, file)))
        break  # prevent descending into child folders
    if len(mxd_list) > 0:
        return mxd_list
    else:
        arcpy.AddWarning('No MXD files found in {0}. Ending process...'.format(input_dir))
        sys.exit(0)


def update_data_source(input_mxd, xwalk_tbl, new_src, output_mxd):
    """
    The update_data_source function performs the majority of processing in this toolbox. An MXD file is searched
    for layer dataset name values, which are compared to dataset name values in the 'Old' column of the crosswalk
    table, and then the data sources are changed based on the corresponding layer dataset name in the 'New' column
    of the table.
    """
    # declare variables
    mxd = arcpy.mapping.MapDocument(input_mxd)
    # new_src = unicode(new_src).encode('unicode-escape')

    # read crosswalk csv to dictionary and validate that an 'Old' and 'New' are in the header
    xwalk_dict = csv_reader(xwalk_tbl)

    # generate list of old source layer dataset values from crosswalk dictionary
    old_src_xwalk_list = [key for key in xwalk_dict.keys()]

    arcpy.AddMessage("Updating {0} --------------------------------------------------".format(mxd.filePath))
    try:
        for lyr in arcpy.mapping.ListLayers(mxd):
            if lyr.supports("DATASOURCE") and lyr.isFeatureLayer:
                lyr_name = lyr.datasetName
                # check if layer name is found in list of layer names derived from the crosswalk table 'Old' column
                if lyr_name in old_src_xwalk_list:
                    new_name = xwalk_dict.get(str(lyr.datasetName))  # get the corresponding 'New' layer name
                    if new_name.lower() != 'na':
                        try:
                            arcpy.AddMessage(
                                'workspace_path: {0} | dataset_name: {1} | lyr.datasetName: {2}'.format(new_src, new_name, lyr.datasetName))
                            arcpy.AddMessage(
                                'workspace_path: {0} | dataset_name: {1} | lyr.datasetName: {2}'.format(type(new_src), type(new_name), type(lyr.datasetName)))
                            lyr.replaceDataSource(workspace_path=new_src, workspace_type="SDE_WORKSPACE",
                                                  dataset_name=new_name, validate=True)
                            arcpy.AddMessage("{0} was updated to new data source.".format(lyr.datasetName))
                        except Exception, e:
                            arcpy.AddWarning(e.message)
                            arcpy.AddWarning("!!! There was an issue with {0}. Moving to next layer...".format(lyr_name))
                            continue
                    else:
                        arcpy.AddWarning(
                            "{0} is not included in {1}. Manually reconnect to new source.".format(
                                lyr.datasetName, new_src.split('\\')[-1]))
                else:
                    arcpy.AddWarning(
                        "{0} was not changed. Not found in crosswalk CSV file.".format(lyr.datasetName))

    except Exception, e:
        tb = sys.exc_info()[2]
        arcpy.AddWarning(e.message)
        arcpy.AddError("There was an issue with the update process...")
        # sys.exit()

    # save the new version of the MXD document
    if ".mxd" in output_mxd:
        mxd.saveACopy(output_mxd)
    else:
        mxd.saveACopy("{}.mxd".format(output_mxd))
    arcpy.AddMessage("Process completed! Please check the directory where you saved the new MXD document.")
    return


def csv_reader(input_csv):
    """Builds a dictionary with 'old' and 'new' fields from CSV file
    :param input_csv: filepath and name of the CSV file
    :return: dictionary object all
    """
    import csv
    xwalk_dict = {}
    row_count = 0
    arcpy.AddMessage('Reading CSV file:  {0}'.format(input_csv))
    with open(input_csv, mode='r') as csv_file:
        csv_dict = csv.DictReader(csv_file)
        header_list = csv_dict.fieldnames
        validateCSV(header_list)
        for row in csv_dict:
            row_count += 1
            xwalk_dict[row['Old']] = row['New']
        arcpy.AddMessage('  {0} rows read from CSV file'.format(row_count))
    return xwalk_dict


def validateCSV(fieldname_list):
    """
    Checks the header of a CSV file to determine if there are two columns, named "Old" and "New"
    :param fieldname_list: list file with CSV column names as items
    """
    fieldname_lower = [fieldname.lower() for fieldname in fieldname_list]
    if 'old' and 'new'in fieldname_lower:
        arcpy.AddMessage("Required fields ('Old', 'New') found in CSV file header.")
    else:
        arcpy.AddError("Required fields ('Old', 'New') not found in CSV file header!")
    return

