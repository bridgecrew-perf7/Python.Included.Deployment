using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Python.Included;
using Python.Runtime;
//using Ionic.Zip;

namespace JunfengWu
{
    class MyPython
    {
        public class PythonSetupProgress
        {
            public int percentCompleted;
            public int filesDownloaded;
            public int numTotalFiles;
            public PythonSetupProgress(int filesDownloaded, int numTotalFiles)
            {
                percentCompleted = filesDownloaded * 100 / numTotalFiles;
                this.filesDownloaded = filesDownloaded;
                this.numTotalFiles = numTotalFiles;
            }
        }

        const string fileURLPrefix = "https://github.com/wujunfeng1/Python.Included.Deployment/raw/main/python-3.7.3-embed-amd64.zip";
        public static async Task SetupPython(bool reinstall, IProgress<PythonSetupProgress> progress)
        {
            string appDataPath = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            string zipFilePath = appDataPath + "/../Local/python-3.7.3-embed-amd64.zip";
            string extractionPath = appDataPath + "/../Local/";
            DirectoryInfo directoryInfo = new DirectoryInfo(extractionPath + "python-3.7.3-embed-amd64");
            if (!reinstall && directoryInfo.Exists)
            {
                progress.Report(new PythonSetupProgress(48,48));
                return;
            }

            using (var outputStream = File.Create(zipFilePath))
            {
                using (var webClient = new WebClient())
                {
                    for (int i = 0; i < 48; i++)
                    {
                        string postfix = String.Format(".{0:000}", i + 1);
                        progress.Report(new PythonSetupProgress(i, 48));
                        await Task.Run(() => { 
                            webClient.DownloadFile(fileURLPrefix + postfix, zipFilePath + postfix);
                            using (var inputStream = File.OpenRead(zipFilePath + postfix))
                            {
                                inputStream.CopyTo(outputStream);
                            }
                            File.Delete(zipFilePath + postfix);
                        });
                    }
                }
            }    
            
            await Task.Run(() => { ZipFile.ExtractToDirectory(zipFilePath, extractionPath); });
            progress.Report(new PythonSetupProgress(48, 48));
        }

        public class ModuleInstallationProgress
        {
            public int percentCompleted;
            public int modulesInstalled;
            public int numTotalModules;
            public string currentModuleName;
            public List<string> failedModules;

            public ModuleInstallationProgress(int modulesInstalled, int numTotalModules, string currentModuleName, List<string> failedModules)
            {
                percentCompleted = (modulesInstalled + failedModules.Count) * 100 / numTotalModules;
                this.modulesInstalled = modulesInstalled;
                this.numTotalModules = numTotalModules;
                this.currentModuleName = currentModuleName;
                this.failedModules = failedModules;
            }
        }

        static bool IsModuleInstalled(string moduleName)
        {
            if (Installer.IsModuleInstalled(moduleName))
                return true;
            try
            {
                PythonEngine.ImportModule(moduleName.Replace('-', '_'));
                return true;
            }
            catch
            {
                try
                {
                    PythonEngine.ImportModule(moduleName.Replace('-', '_'));
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        public static async Task InstallModules(List<string> modules, IProgress<ModuleInstallationProgress> progress)
        {
            string appDataPath = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            string extractionPath = appDataPath + "/../Local/";
            string pathvar = Environment.GetEnvironmentVariable("PATH");
            pathvar += ";" + extractionPath + "python-3.7.3-embed-amd64";
            Environment.SetEnvironmentVariable("PATH", pathvar);
            pathvar = Environment.GetEnvironmentVariable("PATH");
            PythonEngine.Initialize();
            int numModulesInstalled = 0;
            List<string> failedModules = new List<string>();
            for (int i = 0; i < modules.Count; i++)
            {
                progress.Report(new ModuleInstallationProgress(numModulesInstalled, modules.Count, modules[i], failedModules));
                if (!IsModuleInstalled(modules[i]))
                {
                    await Task.Run(() => { Installer.PipInstallModule(modules[i]); });
                }
                    
                if (IsModuleInstalled(modules[i]))
                {
                    numModulesInstalled++;
                }
                else
                {
                    failedModules.Add(modules[i]);
                }
            }
            progress.Report(new ModuleInstallationProgress(numModulesInstalled, modules.Count, "", failedModules));
            PythonEngine.Shutdown();
        }

        public class TaskExecutionProgress
        {
            public int percentCompleted;
            Dictionary<string, int> stageIDs = new Dictionary<string, int>();
            List<string> stageNames = new List<string>();
            List<int> numOpsInStage = new List<int>();
            List<double> stageWeights = new List<double>();
            List<int> stageOpCounter = new List<int>();
            double totalWeight = 0.0;
            double completedWeight = 0.0;
            List<object> results;

            public TaskExecutionProgress()
            {
                percentCompleted = 0;
            }

            public void AddStage(string name, int numOps, double weight)
            {
                Debug.Assert(!stageIDs.ContainsKey(name));
                int id = stageNames.Count;
                stageIDs[name] = id;
                stageNames.Add(name);
                numOpsInStage.Add(numOps);
                stageWeights.Add(weight);
                stageOpCounter.Add(0);
                totalWeight += numOps * weight;
            }

            public void RecordOp(string stageName)
            {
                int id = stageIDs[stageName];
                stageOpCounter[id]++;
                completedWeight += stageWeights[id];
                percentCompleted = (int)Math.Floor(completedWeight * 100 / totalWeight);
                if (percentCompleted > 100)
                    percentCompleted = 100;
            }

            public void AddResult(object o)
            {
                results.Add(o);
            }

            public T GetResult<T>(int i) where T: class
            {
                return results[i] as T;
            }
        }

        TaskExecutionProgress myProgress;
        
        public MyPython()
        {
            string appDataPath = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            string extractionPath = appDataPath + "/../Local/";
            string pythonPath = extractionPath + "python-3.7.3-embed-amd64";
            string pathvar = Environment.GetEnvironmentVariable("PATH");
            string[] pathItems = pathvar.Split(';');
            bool pythonInPath = false;
            foreach (string item in pathItems)
            {
                if (item == pythonPath)
                {
                    pythonInPath = true;
                    break;
                }
            }
            if (!pythonInPath)
            {
                pathvar += ";" + pythonPath;
                Environment.SetEnvironmentVariable("PATH", pathvar);
            }
            myProgress = new TaskExecutionProgress();
            PythonEngine.Initialize();
        }

        ~MyPython()
        {
            PythonEngine.Shutdown();
        }

        void AddStage(string name, int numOps, double weight)
        {
            myProgress.AddStage(name, numOps, weight);
        }

        Tuple<List<List<string>>, List<List<string>>> ExecuteCSOClassifierBatch(PyObject csoClassifier, List<string> titles, List<List<string>> referenceTitles, int idx0, int idx1)
        {
            PyDict batch = new PyDict();
            for (int idx = idx0; idx < idx1; idx++)
            {
                string docKey = $"{idx}";
                PyDict doc = new PyDict();
                string abstractString = "";
                List<string> myRefTitles = referenceTitles[idx];
                if (myRefTitles != null)
                {
                    for (int i = 0; i < myRefTitles.Count; i++)
                    {
                        string refTitle = myRefTitles[i];
                        if (refTitle.EndsWith('.') || refTitle.EndsWith('?') || refTitle.EndsWith(';'))
                        {
                            abstractString += " " + refTitle;
                        }
                        else if (refTitle.Length > 0)
                        {
                            abstractString += " " + refTitle + ".";
                        }
                    }
                }
                doc["title"] = new PyString(titles[idx]);
                if (myRefTitles != null) doc["abstract"] = new PyString(abstractString);
                batch[docKey] = doc;
            }

            PyObject[] myParams = new PyObject[1];
            myParams[0] = batch;
            PyDict ccResult = new PyDict(csoClassifier.InvokeMethod("batch_run", myParams));
            List<List<string>> batchDirectTopics = new List<List<string>>();
            List<List<string>> batchSuperTopics = new List<List<string>>();
            for (int idx = idx0; idx < idx1; idx++)
            {
                string docKey = $"{idx}";
                PyDict docResult = new PyDict(ccResult[docKey]);
                PyList docDirectTopics = new PyList(docResult["union"]);
                PyList docSuperTopics = new PyList(docResult["enhanced"]);

                List<string> myDirectTopics = new List<string>();
                foreach (PyObject item in docDirectTopics)
                {
                    myDirectTopics.Add(item.ToString());
                }

                List<string> mySuperTopics = new List<string>();
                foreach (PyObject item in docSuperTopics)
                {
                    mySuperTopics.Add(item.ToString());
                }

                batchDirectTopics.Add(myDirectTopics);
                batchSuperTopics.Add(mySuperTopics);
                myProgress.RecordOp("batch classify");
            }

            return new Tuple<List<List<string>>, List<List<string>>>(batchDirectTopics, batchSuperTopics);
        }
        public static async Task RunCSOClassifier(List<string> titles, List<List<string>> referenceTitles, int batchSize, int numWorkers, IProgress<TaskExecutionProgress> progress)
        {
            Debug.Assert(titles.Count == referenceTitles.Count);
            MyPython myPython = new MyPython();
            PyObject csoClassifierModule = PythonEngine.ImportModule("cso_classifier");
            PyObject csoClassifierConstructor = csoClassifierModule.GetAttr("CSOClassifier");
            PyDict csoClassifierKWs = new PyDict();
            csoClassifierKWs["modules"] = new PyString("both");
            csoClassifierKWs["workers"] = new PyString($"{numWorkers}");
            PyObject csoClassifier = csoClassifierConstructor.Invoke(new PyObject[0], csoClassifierKWs);

            int numBatches = (titles.Count + batchSize - 1) / batchSize;
            myPython.AddStage("batch classify", numBatches, 1.0);
            progress.Report(myPython.myProgress);

            List<List<string>> myDirectTopics = new List<List<string>>();
            List<List<string>> mySuperTopics = new List<List<string>>();
            myPython.myProgress.AddResult(myDirectTopics);
            myPython.myProgress.AddResult(mySuperTopics);
            for (int idxBatch = 0; idxBatch < numBatches; idxBatch++)
            {
                int idx0 = idxBatch * titles.Count / numBatches;
                int idx1 = (idxBatch + 1) * titles.Count / numBatches;
                await Task.Run(() => {
                    var batchTopics = myPython.ExecuteCSOClassifierBatch(csoClassifier, titles, referenceTitles, idx0, idx1);
                    myDirectTopics.AddRange(batchTopics.Item1);
                    mySuperTopics.AddRange(batchTopics.Item2);
                });
                progress.Report(myPython.myProgress);
            }
        }
    }
}
