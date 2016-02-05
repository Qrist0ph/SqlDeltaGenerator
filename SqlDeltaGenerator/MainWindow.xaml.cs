using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using RazorEngine;

namespace SqlDeltaGenerator
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
            this.DataContext = new DeltaGenerator();
            //string template = "Hello @Model.Name! Welcome to Razor!";
            //string result = Razor.Parse(template, new { Name = "World" });
        }

        private void ButtonBase_OnClick(object sender, RoutedEventArgs e)
        {
            ((DeltaGenerator)DataContext).Historizing = false;
            ((DeltaGenerator)DataContext).Generate();
        }

        private void ResetTableOnClick(object sender, RoutedEventArgs e)
        {
            ((DeltaGenerator)DataContext).ResetTable();
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            ((DeltaGenerator)DataContext).Historizing = true;
            ((DeltaGenerator)DataContext).Generate();
        }

        private void Button_Click_1(object sender, RoutedEventArgs e)
        {
            ((DeltaGenerator)DataContext).CommandToShow = ((DeltaGenerator)DataContext).SsisScript;
        }

        private void Button_Click_StandAlone(object sender, RoutedEventArgs e)
        {
            ((DeltaGenerator)DataContext).CommandToShow = ((DeltaGenerator)DataContext).StandAloneScript;
        }

        private void CreateBuffer(object sender, RoutedEventArgs e)
        {
            ((DeltaGenerator)DataContext).CommandToShow = ((DeltaGenerator)DataContext).BufferTable;
        }

        private void BufferToStaging(object sender, RoutedEventArgs e)
        {
            ((DeltaGenerator)DataContext).CommandToShow = ((DeltaGenerator)DataContext).Merge;
        }

        private void CreateStaging(object sender, RoutedEventArgs e)
        {
            ((DeltaGenerator)DataContext).CommandToShow = ((DeltaGenerator)DataContext).StagingTable;
        }

        private void HlpNextOffsets(object sender, RoutedEventArgs e)
        {
            ((DeltaGenerator)DataContext).CommandToShow = ((DeltaGenerator)DataContext).HlpNextOffsets;
        }

        private void HlpRequestLog(object sender, RoutedEventArgs e)
        {
            ((DeltaGenerator)DataContext).CommandToShow = ((DeltaGenerator)DataContext).HlpRequestLog;
        }


    }
}
