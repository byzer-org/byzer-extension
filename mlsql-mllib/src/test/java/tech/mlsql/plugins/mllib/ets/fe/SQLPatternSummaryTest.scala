package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.streaming.SparkOperationUtil
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import tech.mlsql.test.BasicMLSQLConfig
import org.apache.spark.ml.fpm.FPGrowth
import streaming.core.strategy.platform.SparkRuntime

import java.util.UUID

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/7/13 12:48
 *
 */
class SQLPatternSummaryTest extends FlatSpec with SparkOperationUtil with Matchers with BasicMLSQLConfig with BeforeAndAfterAll {

  def startParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.hive.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=metastore_db/${UUID.randomUUID().toString};create=true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )

  "DataSummary" should "Summarize the Dataset" in {
    withBatchContext(setupBatchContext(startParams)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val et = new SQLPatternDistribution()
      val sseq = Seq(
        ("elenA我的", 57, 110L, "433000", 110F, ""),
        ("我哎abe", 50, 120L, "433000", 120F, ""),
        ("AA", 10, 130L, "432000", 130F, ""),
        ("cc", 40, 140L, "", 140F, ""),
        ("", 30, 150L, "434000", 150F, null),
        (null, 21, 160L, "533000", 160F, ""),
        ("Abc.中文字符#123.01/abc中文", 22, 160L, "220", 170F, "test"),
        ("AAAAAA,A,AA", 22, 160L, "220", 170F, ""),
        ("zhWTNEcfRuyLxYJBJGenjcxTGSbVeZPGQPPFSRRNyiLVrCWzHKgXHRxvKqizEYAcgkQScuuJAhDqPNXSjYnFlNZUMgONOYNrxwfhJLWIjAghJu" +
          "hhEWcJXiWApqmNSJruaEmtXSTqZPOjIHDGPINeSCwvfMtLOkOkUFvaeRiXsYFaSfHjQGfsAOZtKQnRupNlKUaXWsIpiOLIeSZlzvadcCopMlh" +
          "KClOuBYOhuCoZVcKkDXJnNGrWMpqQgGtHIwQoPgieWFcRHLwSbqUITyrrFduNNOsGwgConIDXgcyoIMOauzZOBptmhaKXVlKexZOHAzphkkvOt" +
          "LCcIqFqqPAjcNEEgWjmRoIhIZupbDqbQmiVeihtQYHYbalPRUeOAYVVdtmUuDzLhlJebqZUprQoIjGigaoAkMiulLlrXkTjpSFAkyrZODxLoHoh" +
          "ICsOURcYBjASGvgtniFuFmDCegAmteQWsBvjetIbitJMfNmjYWlkyjGjIQiJSroZdzXXoHpHqNRsgGThxBVrYPUJdrQFHrarblVnohkcaskpaby" +
          "lBNPKbGPzONqfZFnolzIfZmMXSNwfxzaikRThlpCgWIeFhamObHtXJGwYIdGRfnfQLwPrEhRaOoPHbChXhhHsQWnEoLjtojGmJrkIYMsfjb" +
          "wYBYltWKNIaCHEhUNmdBzuMmEhVdKDwzgqAygXGAHZirygrlHsPoUyZkNcgDhhNfjUMNlBvMECSdlFSeSYoHRyepxIbdMxTWWtZahc" +
          "eCgILDfVIbMNlOQPPPzIGxvQGKnWxthQifbajunjfiytsOQdZomwLooBUVrPFqOIxRqWimLPEVSxUzvvMXIoGWMxcmDsTzwCTbnVgjWVEtK" +
          "gUVhiROLOgruXYlfwGrKzXCqLfghSlHxxlhPxRhfIOxvmsfNGdgHFdPgeURKNjmVuWvnavEzrvTcMVwbNRXePNHFKSERWhlFynJMQEJkf" +
          "gfuiRLgjaDPEAzUFbpWkNyePCmOgykAwIugRSOVDyEgWebkmNbdSWhSxMGtwJkBmIDJuTOnaVVrNXUgkNTzgwQExabYxOqCocgdvpuAmuIG" +
          "xzsaEYWwvzBYuvngoXSAcppdyCAtNoQrHuQlVbxMdwoCmKjqeBHeWkEMUokSpAzbShnEVODQdnrMABDAXLPAlnHGeWNeOLjoApmvYxaycuU" +
          "FwDiqHRsSFJAghwaiLzSUiBELvLiQpIrcJYLdbPDtjVxEeruUWXqKrgBjVZXONCqfBXZwJNddXwGkFBORwhpPQETceZTVHoshGyRNQNEzX" +
          "nOlZAvQUkGSJUTBmxApxzOtntUUYuXPAhVYLZojUwPlZndAokjbsXyBKeiYrOjowRXRMrdqgdgVaIKFvcYfnxhsTjuTFyesKBvidjnSxxa" +
          "oPspeTUmMlcqgVYSaqGoSbIYQGibfDaohsoRgYbzWXfSmvNBUUXuITCgInBdCRFqPxFuvtWOiLGaHDZQuUsyMabEWGmDcRytqtSawWj" +
          "qByvVoZmmYGezgQMRvGQDpkTeJCBvCvjeSXkIYuuWvWPzeZNDHEeQNmywpvjVzqSXufGvPAtYuvnxcSCZuPHOTovHkDAYtVamLsbvIlP" +
          "wwIpmFeYeOvczPwxdTlXxQjPdUBDruEMiYXClrozSabStIgwlrhQUxelIDBAReBGTSXipfdOpDhPqOUFZxzYOPCnYTvmSjvXseFSLunAD" +
          "FeIqNLMEirwlsqUxGAmTxOcqRJYgtcyaqESBpbNioywbfXJRDEkcjnETZfSwSpyrPxIOldyklgMZKySIJicSTaKWFAlfZdXUNVCkogaUd" +
          "dUCXRALCyxfiaxPcNtLjcvALswuQLxiweLJItsVrHPhqhTDincsDImPcMrSrlPgvLFwXUOfeBYMGZArnBhaUrVBgkhkVXoEYdGPJGAYwe" +
          "OLIsyjzluenLTEkIjCcchulmwCQRgPDyYdwCfucarDDbDbKwnWCDufGcnZODiLVLGUeOUoNIACqtgzigWyJGLpxoJcqgcTLVSTbEZxbvw" +
          "PNPXozqcsbiJgzUKWuEBgVZVZTRwIoFPSnEfVVEpFauHAPnkXpbZIjcwcbZDdURCGHLYTyUUyvqgkerXHslisXyMQWpegiLdlGgHwgqJr" +
          "euhUfJWxNGUBYwRTvLmOsMkvFQQpUkoZiYJLKSLIPekyHPumfKroajwVMfMSseHXlAoKfcKkaWqJzLtAqaftqTtuaESckJEdZaOcHHEuq" +
          "XWhSCEaSqFtvnPOebWflufjZixZjXHlWpBVPmpAhTYXgUNKXYtOsGjhxTanupGFMOtMNiqsnfpYvJYPceGeUfiewNrKzFxtwIwEMbOoRR" +
          "EQpNfhtLvGSFIbnmzykkUJGJvxgtkuUHBOGppjrzuqCTbEJhuiYqhRJBpMpbMJBxPMERfBLGttuquvIooqxnFNJmnDmcjwiIjHuvuwUWH" +
          "HsiRAqOghBYxULRJrGJVFgsRXgyMbfHyIiJNiiAkKPdUsbLcLixHCbguqxwSEEUqoqnvLGGyZSiBSOZSMDBUaknhcGGYyKbOeBgZUEaqx" +
          "eetNaLNYhchqRCmCOQjQHscMjKgfHTchLdZEbSBlpqAiriQhTDzkcStZxilXxPsYAFmegNJkcXXAtkaUzWOsDMZaXBomBKYXFajkibqOx" +
          "OezjsqVNaGWeFGsJsPnaAxvvnuZHCCVJIbdEpBHaiyzoxWQYxstqXnfCtVgfbMLIjGZwKsiOYejJqTikiDIrCMENLfYEZFAKuwVZbSZpf" +
          "shSdvMGSTcERsgDgjWGCBgAlBmrsyXJddQyDCHMtjHnXMqEkRBhWalRGYBSvygcxxVhsYJqnoqhBHLDUvJbNvwOVEsiMYIZqHWArXAiUM" +
          "CxUtbfxbdcMlnbyILGzmVNhjiBKKsGxGvQihCkuYkGDqmcbfqkPWsGjMVLFrfMOsHBBIlibLdMLeZtcBwIVEdJTzRvIOjUUsBSWMgKFhH" +
          "ycimtrvrqHwYSJbybanYmBJaaohMfkzzRoYDwAhXZTelYqQlQxwxQExUtuKdBgniXXxAHOmgnpofFLwCKcBUNHzbJRedCOFInsiWCycEH" +
          "BtVSmZmqdlYTMYTGpaseNdsrUWLQoQuiQUQBriRzZCClrFXiMKYQtFUUEGJOzFhbLeWZTagyqgvkqfeXeiOXGMWxzsOYkVHITiyZZPuSh" +
          "rIrCwVYanITUnIqYWfbpISATpRgUDMXMTBKqCcSnlZlZLlGbOyGuTJZJWNFsjyoUleChiLjWhXjXQaEGOpeMTCeremOJhGstCrDxBfneT" +
          "kiXLANpchLIdNvouoaxfmpKjOxOaCkKeMGbbNaXVqHTGylOiqgEumAiiyeGyWvQVuveClISzisdgwkwVFyKNQEBzSHDLOHUhQFrRAgvEV" +
          "kjYgXanTMHeLlfRkmuNIYYJaynEfzZUmfLPkMeqNdSwJLWHiMSIxYlIrkfsvdIyOFmSlqgWVHmQltPFoNDLbphaHFDtePWMoIACoEyMBF" +
          "YqzYKOHVzdcAXANTYoSpjSSiSCfWIKMETHSDGtKfameckKzGgyEDRCAVKksPTarfzinbuBcgUvfgBMaxGjQKarVLXEHgyzYxQLoqfqjGB" +
          "xpAnMzPztTpjGUIfkkWkZizMWWEelJBAypWelZVxYktZjsKjGYAkZUERrczQnLhpbFVmnFZbnquXKBeGPBQuDMDWaLdXPriJifiLrlunb" +
          "nUZbzyzxHcXlqcJSCxwvDZUBFYaWWQSRTDlpgAbFcmnTvqiGfDbXFTXOIumHDNWTLiJJJqIOkvEeaJOBljWbdOSdMthKxcZbUVPOErPpT" +
          "jlJSwyroMtivFMuORGyUZtKLmbJUtZJvSZtPingGkQFCrgUBSPLmNWEFMOHaBIrFURvKQTgBDGfJvbZQGXURzxVvgAvbGAvMPhtIcTtrM" +
          "IqzWKbEjLavBJoywVNStlwquIlCYiLofSTJagErCFnCTLIOjPsTXSwfTudxIvNxypAOqMykcCMcppMYhWKvCVihvBaPkeTogTwNvopOCt" +
          "eTjhdLthgnEWQTPxKoLDvITjEsttWUJOiJvubkRmgXSrSIGRaqlxpKDHnMVMdUtGGFYrPrksNlUpSwNdmJfqkgzKOaXwvEioGrFzEMcOC" +
          "MMWqbsGpbWxWRhNWYFbDoFhZYXtRsJBezqGpjgPjNDtxSLfiZgOuuANClUezHtZiVnhvLcufdYFYziZYTHzJrrRkHavijzDOZZnfIbKdK" +
          "qKvFldkCUbkYHqmknKnbmxndedhrFRobyhFfisPXDeZrhszJoJhOLkMDLKDeTNbMYGDVNPnqjKetAjtfdEwUtLxWbiFBIdwVqvafavEjy" +
          "QKlhEFULdDZXUKQYEQfrGmqrcEdNhZErzUSiaRIVMqGBqsALKRRKzSNgVIKZYjUxWNCTLVMCsEzagErgcOmXbpBTcolyZWXtCAqxPlrpb" +
          "pJcrqkIWfYyhxYyOBwPKKVJJBadhcsKtoJBHmPJdYpjwkXdETfSWBQDHctKYeMpbxLJnqVhyRsCtpLauPjnmSsnocmgVsueyEybIjUelCpi" +
          "QpgLPlelRjUaZCmxccIAAWarApiLwEkVSeMfAhbEbdiHJeGKAXadSwGTAXALUsnhtYUCgLuWepDkRSOEWfahzJeNmIPeDDsJjfAYdQyUIVc" +
          "JBvfQiLODUVTNyZLWgMFrCNzvyhtUxWOcbGEOqidgUwiCFvErehKUupozTOsuIBwrKnYSZhKHXnyQbNIGqfveQSTHEPCUViNRCVqoBjIefd" +
          "cmmrmuVsFzhhhGbaDsmVSupwBCSvMlwwBaVpXFOAxVAwEZAWqVzalbRlydhuibmiGbuThvqnMjALbJVVQsPYEoxFSjsTrySyQJxTkhmhSjx" +
          "PRMesWivmbtngChPQgkDakOOPkllDngSEFGOjInsKsCRLqJDhvIuUhbnUusHUkQqTXQqFKhYYgeazQsYgxBlDpEQNSPSNbpEGmqBVSTLvjD" +
          "ktiqGVqlgzzrUhdbFLiuOQbRXqMOBOgGiYSPkVXoseUpOUmhBZszMoGwJkcGOaTeqHMgDSlBMmwgOelYNhrKoCXwsjnGtqqvhprUKBVHMz" +
          "OLIryMupSqxKMlzmeIgcefHeooQRZjnMFjqeAJZRlRkmxnoSvHkuRciiarBqyMWbzhojpJYWvDaAqaexlUsKytwYVCVGzIVwHlmmQOrIMQS" +
          "uMUAWymVKkRCMdtJCJIUkLCKwAOAcwqPkuldhDiARyWVrjgrevfmQoTfQXdCUDzdfAcRJsxDNyUBwfyfJZBtNkpHXBjRrtWSgfZDTBxZedA" +
          "VDlGXTAjzZuEcUnUJMKXlBJWIQzzklOAYpPuxAqDqqFEovpOmzYDGaHIUOkzrYnKobgvLuHxsmDpDHipZVNUntkhgzGeiNrJbSbJZXIlPFSc" +
          "rxJtWKlbHWzFNmenGpITopFyCkaLnZNyPEbGlqdKbvUvpXJbOrtbhTQbLtPkcJRGltRxMJJUkCbjRQgAoWrdGRcSXdsGaLgOoFwuqFuIIkd" +
          "mPygsBSYgmPEpZJVVqFvgmgGUpDsRGcJMvVdOsvbfNbdGenScXgYezczjRzKhHLukNqGferLRmeIUTiSWrfltAebVFaozMymSkcyYSJIWaMV" +
          "PtUZhtqYrHfpZMedWIHfNKfUKGoEfLWkQsJqokULvIZXMfhuKVslBNPwLwFBPUuvHkXNQSQdlmccRxWHHLcXvkoqpfbsEHJZlhBdCHhZZPA" +
          "bqCOUyZJiViwgOYOvHGQrKvrPnWMXjAJMnsKtGQkbaWOszxbaUkTXpcgWuRFtDhxguiYsegzqWkuOFoToOiuNPCKjFJkryAgQIbANJKVwgE" +
          "IExwqTACbJpFQCbwmZVRAxbvlTUllkkBJQMrwJMxEeDjvmqaohOWcZVgnPVEdlaaayfmZrYBdMbyDJBXodPDHmMiRNdRMsZgeeHrNZdzRh" +
          "AHCjQwrMVEiAOlGjijPUVhMVvXzhnDncpBcpGVIoetqhCdhzPWyvBhHziOLprKUyNVeELpzzLfbNcpFsaGBLzwgsLmlDxWhpxdgkZaXYSg" +
          "UBZgNoHQYMDCkIsSuJSiXBPPbXeOjHXlKSOVXqIFBIeEfYTRoDHwiidKDZYqgApUcquBqvTkCmSxmiPCpsYrBpsstFZLwbZeTqBxDntgo" +
          "WjLGfkbbvUoQcoyZFBqXIXYMuTpTJGQNdSfsAvRRXoHaXcBXtYnOQgcdrbZrYNANQlFEuJwPqYQAqclnUiwEjjcjidCtPZwpcdAQgTWaN" +
          "JsyhKwipnnUOrZnYrqeGerOpqbDkBeBWOKnezvsnjQzTIOgfagzWeuRMXYSnqtKvRrVyqmwljaTRtBoolqlzxVcitbJLIfRDzVOXOhfovA" +
          "fczUcLPfaGCZYzlzeuoNZOgLvdGnxTUSCyJeejTlRRCmcrjrkIcaBqGbNpqxsybkFrYRzYuUZCcxBmGdNImtivKdTUBKKMwYfxelBvSkf" +
          "DCgZisTOgBtqTBDwAebyKdhsDtbPlhuEEZVbrveWjBKjvBvHZacwySqjzZOHuiLttoBdaqruhjnspYWdImpXKMDpnCmyRBqFdUlNItxN" +
          "NTVOyyNKoOYyhIqUGKlPjVSbeAgsKujOcLcpsDcuLdptnyUxNZvlsKNjmJDrgKIVROtcDXwtHRwlDmpSMZFlIWWmArsdnghdhUCdsYJ" +
          "jiEdNTWMGsuyqxcJVGYirZGEPZimlTUWIQEKioevbgXrWIouPJzzfrfQMlODnYBEFvurpRgkuJwLllVNajjXsHQWdYHYMUMoazaxrk" +
          "MtWjQUlVWaOYdcZMyEUFmLLWgqhmLTrAcwKOZFyCiYFPsSAlbxOeTCmGPoUbsZKKJJtdaqkwzKnYMtFdLxDHbkIRyFLuhFkDxFlTt" +
          "epRCeeCyEvBPsTwJEZinfavDnuHoOYwlixPTbzFxTKLmrcQPRtvkSuACneSgRTkMVEFPjixjwrhvvrvWqOrjJKaYvpArJLMCeKIa" +
          "baWxUSflbhcfWVboWgDOcrKPNjOllqeUoWKtCDdgaDsMdOfEdnBSzzJdQhjfaJaJAwMYSsXjIAdfTfqolWxEfnfTvKcPtpVQrMNdq" +
          "XgiBlVyPigLpczLuRnCBpQMHvMCvILSFgpdqQeUCYSnCIEpORipUEnwKcEqlWZlOWzCkaVnFtnCMaRRMElCyADgRRvqIZeCWaWXAsZ" +
          "AuhzfgbiukDQTAMKtOkNcQpQwKJJoXDEBQLXXIdnvdjwmkrAnTHVvTgkUQULwRpjjunpeiuwMMjRLnRhbYngbNXqBINfVEvsapaNiPn" +
          "MEgQCMUJaqzUvJfVsGtXVJUYajRVtuRxqPlABLwoMqkUpyzzILDwiazMdaoaFWCNkRzfdEYxcmAblWTPJqTaRAmwdkTCCAKFfaifNDl" +
          "UXZowBQWhODZjxPXiuWyINLibFaFgBEYnjZtzXxKsxeXzyDtQLMMxnlwkTMseDUilNUBWTiJNjRnvWGgfivhDPvBYyipMWLDcsYvMgs" +
          "uVdMEUhdCGxAtlfwgxYpzSgekAwUhQKACokqDBjMaHcORAnvgvFSvjYeIAHIKPxRLnrPZHpeDaxrgIgIGEJVTqLOjWYWGnyRAhSCySN" +
          "wARCEJNWOaSsLKJMDCNlUJecKnMtcEXZVbojbWCsiMZIxLtKgodHPoTdvEkVvCpyicAIsdblTlzIHiBYjPzwPIeDiBxvcawurYQpwt" +
          "lFCYQaVFCNhnHpSjaVaOKsvbzzENrCSesJkMQuQpVKDWLYNZoMsAsmrGihnVTSCwuYgejsDmDuvuSFVujJZgrsGADtYfhubUOBWpusr" +
          "iWfMeFlUeexYXVhIDiHAVWCXIMDqKhfRmHweGaZGUIjWPqiPpWfQLXWfTHlPAuxgBdgblXGsnvnpMiMmQRFppVgBMIcxOgwvMBPvhAULT" +
          "xFSQlkZDdEPwWqFpCTMWSsVHUMenxZQNnRtiqwrRnpnynADjLtiwYxzqTnCQypoHclADPseNirLfXElIOQyGKdJvcXNpweRbhXkXswDdn" +
          "tFZlrFXMrkJotForuYmleBMdyxfqshgzMsVUuXALexNaalEozUoOpxujePZOXjFhWufhEcaETSVzbHhqZdiUqtPFMIXkRQOQGbdeuvBT" +
          "JzFHJrtIHIuBPRgFbVKjglbfQmvbfYafgDbfGjlIBPxJdcTpRkgJdDEICuHWaKFcZhdevlByUpMZSlMKKbwpBfLGBotEwZfXrXyLpDQX" +
          "sZojHkpARppEKsWXFpprkyapFQFWjHtOKxggoAvtirkyTmvWsFVVbvkVwaAZQGbFqqixlLjhJvtTzJeFFoeDTDQWglEVtwzouQDBHuu" +
          "cdayYBiBiijcSPjQLxeYpnhyVXXSJBAUdQCyuslGsKGvgsYIZOMNLJpmuJdXnGVjGTeRtAPhJxbXWuyuGfEGCIFkBKeWUxjitmeNxL" +
          "tdivECPMwSGdckxLEyhQKVMpJmWpdzADcKsMHcTDdIXdQWbXTTRCHgUaCKiyvAcxRspPGHflskQHuTCIiXzEbSGnqTGgXojBzHMpYAU" +
          "wndWlFLLhDqlktDiRSkjhDCOFLOgPEkdzWOojUxpIesDKEEoXpnjMnQEhRQmhvAICzLIEMQQwTqVqKfVkOuxQiFMxsYZcPFXlgBrPoAl" +
          "JsvWpvubsxVRQSnvDguhWYlJdJPGcaFbfxYHpalKbRsvOKHgwEkhMngKoJPuBoAdkLCQVMhxdvCVJZvLvLFZFKExChCKRIKtGaicHy" +
          "nTrAQNWJmEAtNkFpmuNhtXNENEnWyXOuZIPAXOPFtQTxBVncSXcTFcSzbhkLXZZjYSxCBaWNnmcrJxVMfuYeIzxtmHJunKuslHLgqO" +
          "tBfxRlmWEpswszHCvlPJKFsARiLCMXSrpxKpbWbUHXxUSUSctxGiGxgaXerPixloLBqItCxuMWRhWHpBMjWbaQyYwAJPwliPbsxZRLHx" +
          "SpJyPltbtXgIYjRkluSoJHHFMFAAIckWxmIMNpmOkWUcporoiIHPRbrRCZlfQpenVcodhaFVHKLgfOveHiNPzSPSWadoaUsFubbjbVEcQ" +
          "TSTfwsGiTBHEoWPsRnQFumvqHHAySsvOxNmlhzdgTVVzbZTHMSebCPtdPyciXlLrjRgRfbaWtQAIYDfkgRRTRoJLaWOJlLCwJHhJSUiay" +
          "ZCyGBPsHdKRwxgRdwhAkdOqXffGJKtwdquRDZcxMrtYPIifherBTQxQyknNHpjFHSZBLddUICOxgNvdIyzmmTyTTpeIzIKcRtfLnNunjF" +
          "DeWLMAUrexJeVyFjNNHbkqVOjfDtpkjOrRnWpnWtsMTlBEpovAFmyxipTPmDpPZzBMmdYTRJibEaGGLwJIVgCqGvzZAjfgYHeizhTOcxq" +
          "aXGIhsUIUxeRvHoiYhISbajcMzaGUmcoALMpRqavHaQCssTaOmFQrEMHgupJfpybzerpJWjISUFeMuTODwaCbnyyowiXnKwwqcEoiRsV" +
          "rKqcGADPtOVUwCibaBkysotwiuteTsGAdxDeIMKkFbiZNXIxvTpBbgVNFNjCTiooiCotVRztdaFEmaKvXAhApFjHQKYjMysYxQRDtBWrw" +
          "xzElkLHcoDuGTmLlGObSljCCmZiyoyMwOlRNWMBHYPOSSiECUesfdUTyHVOHvjHVcDGNkvBpVyqDbCKQjeAhCzdvgGKqLkSzIKLnPkldpz" +
          "TPLCZSkvBpAOJqcUgSHGmZxDFKMUDALPtqxgrVxPdzoQerlJWaZNryYvGtpOfpDAYNUMuOAVQzdiCtSzrkBwqpVAkKOGcCJefplpQJoPRU" +
          "IEPIkYhwmQcEOZTkCUaPNKpRiIjZeIzNESSUrniYFgfJEPJcnqMELVbLXswGxWChPPZGjGQPRQkuSUaZMBRDzJjdIWdcmawLRwCMeiJaBe" +
          "gseXmmoHKWRyYtFaETyPEqPoTuAlkOXsVSysWBYYUxhwmLNWrsqYfirIIQmxFxhSrqTtUiYQsEeJorgLNFLQikwuggVdfPcSovCYLOecJfH" +
          "PSbgjUxAbMNiWMCEKCykoayDLSkKCbiJBZGuAUTUozCZFKnHwupijUBCshZGfqbZrzPfbcpommnHONFoOoMQKGCduwekpHbDZhRqGQfzobo" +
          "EOHXWvxtzkotAhUHYvRsgCmLqXSVBPPtgyXbQbzxNyvwdwoQJqroggfbwIOwJpsmwDVFXbVPVQLQIMNPSbXSVkAHHheZuHWLlHWZWSqSqk" +
          "WRwAnyCNgtgCnAvgbxmfYVaUNwnLyVMWiYeTzUDsrucaziqjkkFHJRIlTydhfTMEUjvTDldnOdvJXuqCQsQcYsTjoQoEiPPQPaqwbrLWq" +
          "AAUPSAceWSPPCdQUWwkkKvQwVJiPdETlOTEMEmsyRveAMtvizuacYYCyYSPsYsLCihslsfLDlmVQmujREArxhzXUiSnveNyggIeVmJISk" +
          "IBaBuTsDEYKQOHRYrFsEctoIPthudyjHZRXqpXzbtkwboRtRUIwuWOoOgGHvhTHKKQETbAtrmzJmYcWDdagAqOfHscIDeJhyHCciyJBG" +
          "JTHMREISXSgATjJUejWsAmnCDbIfIfbZetyDZxjTibhxAxwGvznYHKvSdfSdmwDeVFibXjqlKWxEjzxDtNTnnPAGDkXckYRsCWFCEoEN" +
          "nNlyzFvyFXQjbWnfMpdgiTyTrrxNEgUUMDsuxNwSHyqhLVsUtXcCoVKNRfzyJZwCGTLPITwzmHvNUCHMqvUGHXrPOlzFuIsjsmbpIaDgEF" +
          "ylxilBQIupZXgEpjgelfHrBLoTXOZSFKJiumWLKeSEyEqieHSDzeawrxeqiOspuOqzFgKaMXCmuocdzMhytjJwPNXuSelfqBiEJARxugjdLNCDfnNPpEQCAeWwPgUyQMvLeaprFNchkeLaYFNCnbJgugstMGhCbMWcqVLnQPjLMDGOzQiPzmIksfeumrtPMJZlxCjpawEbAAPzvNtVmgXnsWMAwnLNASePcrAUGMMnPFDfjlavOgGQzZbSgCsWtiFNkowLnLZzBnZBveNZGhQeiTSDRDqgHgiSFGuQfxTXSwRgejqYPJGvfKCSPGcPFEweIrVVQTdMnPmrJWhoOuqMXGdCHjyjtGrtsHsngppaYFOcLNstnRIrjuVctzooZozyPlFjWRcgwPbCVAnyybayHGUcaPyPzpHqlbxdXWMcbtJfSbSzbTcdNYSdMoeeVTrFcIfvXemHayzNiCncXODETQuXDDRFUUgeTCzMrHUitdvAEFSWNYtMhjwucmNRsbGVORxskMELsBNviebqBiKOnmxUtgVCNcGBLPFgZFzoxYiOcJJZaSbKupCTxeGfCOgyRDXoOjyTcPvaEKABgWTZGrxuSFKESbsFcdKPqpskGDFddqBbaduhFScDHtBxbXJRwXviWYMIbPmNWwWymntnRyzAFKkFrKCbUQTdsUHOYKkOXkKtnYxTpthESSHYsGEcxyoZTzPpvvuceVrRpJjmTgsehjZkvkQIaGVEkCCXjffuhOPJmQLLWSXKPPVzwWIFvjLGeRXdcYTFeofUWELIpnBGmzKgWWFANmIaDRAhWqfiroLwPPrXYpNvEXfRmEhEffGrQpBDJJnoFMZDmnUhzwwBXThPXNuCEcdLbFyGUhdzVfotFmKpJyeAhROiFmOdiMILWcBYGavMiqppzJjqjpXDnAcLVofEpjkAZrctcmkQQXxAtWfUVUAFeZQVhXJMIDCbQLphZNNLsGEQclsBxGmBCyCGTGLHiaLSKioFprrMGlGpwQMYyfUkGoyaHvmbAQzUtjchGIbWCKqeQKQHahRMlBRtAHeqsNzlXsQymrHYSpZSrtgZImaNwECTfUrecdstXZRzPFTOfDVFEwjcgBmRHSQlIdVHYXrTFUNDBgcStxBSXVDFYiPmZltsugzUpGGYyYIEFSkDYBMzErIQoqXeUrdikPMFFwziEWEzPwNNRvdPRBOxiHJGWOgCIgZAkDCpchqBscNghgByTaPgmnbnaTnTAbpPNLhVaqVBcaKItGJYbFRcKKhEaMAMqwbLxVkpEgZcvnKIVgCjcJXqdwPPGuAeuRUjztcoWRJlJKVnYtNpGzbbrtrwgKsrCVmgttsPOZRBLqagQGmayKLUCKOgzyGiDgsLhgSpGIUEPKfBkxZOSGdYMGBKhQhrUfJYLHLZpyqOfscUgUvcjxFvgcfLvaWUGOaGFxvPDPJjqakmPwnrgyFYbMAxyalfBmXDZpFEfGdyPTVCdxGxbAUCAEgOmmFUskWbIqhZjWXoSbcUdfwiIXvQogLOJbWFFUhShqSjJTrlCajhfCvDuZcmOHLxncpDbLxKaWMcwdNnPSBknVgyZkMRhoETfmOiitlEEBnIyAZJyqPIkaZAaMqIBWiYObRGEEkNXBtupzieQzynqmjqYqKHwGpFwgVPQHTiZggJkQbHIXcmnHnqMGiEnTNIoPmBQWoMAtpDmioNHRjzFTjIxbFipQiZoTdTcFLZNEepORqLFYiVfpSCFKaIUydjUsrGUmIzZYmAHvVoyClIjeuzGAYsnbGoXjLGqXUlmHNMstgKjmjJtiNZkwgJBkhbdEwBWZPBOAaczOfupSWNrKnepfHYWTfziZzmuwNrDZJMwXnyOFTpONdmQgyIxLvkfanQEKKgVmVzAYxoMarLYhOsjuYzyzZMOZdLpOOAZqCFXwbLvRHougidWDSIIUnrInSopGAAnwUoXfdnMZtykNpmkbblELstWQymtqtJRJkBZVJPRAiAdoOcHNGEaWsEpZqHCQdlwrDSKvNgclaLHNapYWRMcaAwFQaxrohJkZRDAaqALNkhgNSADlIFOAMOrIwBcHdXGesIPEDGIVmjRMErWRaxIDPoVMkfUrbSoiOmxgfwhvkVarNTbpJiNTfOKbDButokGUASCECUXdYglpAJVOdKnccMRcLIzfohpxlOaqyYDvbzKAxUDuYxqfPSmXmyewVCQbfRcKlCKebvEAZuktmIarNulomqYGlWhEYzbocNfqEvXaCejHRSGnADyDMpxmFhjStQnWpEBEgpMKkzRTDmILVRsebzNbqlXhMIhtQbAjGkTdEhyZnOBFmsDPgZCzGkTAvzmKNNkBzHsAhzuTEQUmKwYgCnQIEcLIMOaEYpUznbUwefxcZxkcFOHbqAjgjymZpDTzGTBWlqHZkPVbAfbwvRkdzYeZYxXQcCDBKBtrRUyXRzXThnxxcoKwtRPOgrXKvfaHykOsxzjTaPomRQZFyuKkGhRwdBTOHWwwKbtJladrXdMIcminqqnChMfVFsuURHddKrBbbSCGLKOpCfRryZjjZLkZFvAbZDbdBDrvWQvmSEYEcYAKaLhENiaKFrxuVfPsnWnwCxtkPSpYXQqZZwwbrVavyJlBTKEJRmtqrYQoaYEumgVqgrPyVsYViiKEJReYsGwyMhlvuWjgiMKyXkRCuSuPFUENusWeWHRPTZZOlVfqVhxUAsTlCQikTallwCbsWYDKTyuBkxsPbfythQoAYOVzOIFRybMyFuCbSvGCsQelUUMhFwXEHmZISznwhEXrOUbKNVYCGpqNUECOnbXXFYRxHIujUVnRjcxHQYMtdSpfYLaHEdILxZVJxprfzSCCxejPIdzYpcdFZgQVzuGtsSzKMuJuwAeVdAOtWjVpKNRWHkqUauXcOMCOSkcZmxaDFdpCFtmairnJIZBrXsXaITRqccmATYrOebTQdpBnXowcPFaMToGgbGeDHXazpyUBwOkcQVeCQTobcfJeZgiqbrpbSHIXFKtaJWuOiaJIJbfNAmpLWsVzmWbYIWegADlzIlTaZtgOfiDoRjSgzSphocDLsWuEesXdQAtyvUgQbypNvPtVvWWGXcBYuCBKAobpqABgqwyVQPrzuOumcHsyZMcDPifGkFqMUGFAPhZOkyjUJUHzEMHrUTusxHiPdVBRSRFwteWuUeXveKlKRGwoHhueQBHKdqEiOVDXiLsbWDYxCNnkwMlZuxDWoVXqGJyzmoLEGuJxXVXSLyMSiViywEOWoMUCwVNnvGjWLLKXyxHMsdQcOKZhwdHtNeYFELCnxyftqxqLeBFAytouSxqAonVCkxhhqRnngQzBtxsquDDbgtpvKkIVZmTwtNtUPtjpJQWTODHPruxnRODjVVlynertSdTaOHLSfGXsDqcDSLgsnFljXvweCXptLEvMmLMUpDDnJFvYTNQkJoOyYwWKnhPfgTKqimWjOQcIQRBlUwFCEyGfmBZioFMIhOkyBhRFmULbJbVYlszRRnEJPdtYKuLcEVpHqDoBNyFBApABlPRNIFRWyNOqwDSchhkKBNlLOAnPjdAQpnZVOJzrUPJuiKRYIOeJxSJDIhfMOySFnhZonKCjObBEOOkiCrTUZroQfWkFXKHWkdatInRSQWBUEPujvgZumaUeJXvezoaPejxxOYndRNYHikdvEfnemeYdgvliWqYrOGXrXTojGqBlruSMVMbJETHehPIIodHLEZdzlWOMaGmHlZyXJquPDwPJEsHoHenkNfHgOLKnqMMhMwEFszVBnvOvhCTehIYeUbHjnbzDxhNvsiLJgpKHYGRDvekwMlnXaURpzqVTQCIpQgYSIxVxioyjMvNsSgJBzfHoyiYnnPMfvUkgfJwLuXTHwWceLljQWYgVyqKswKRzHZKcygwgYzUHjGCOljIvWDaMvpQHYoYdaoJuklbzRLoGkgnYjOxxpEemaDUgaZQbzuMYHJqKQpzOaqfABMloMgOxCwHDeKnuYpuUSMfydJBRKAHVRgGDxZHWElknhIjhqtCIwWPeYYIQKAtTvdgiSPEtRJdsFddrIHxSKEdFkdFnTqokWvKhuKbEYLXxBoYccEnYDALyCboLZnqQHjrnXBITBwiDQPVEKPSSvPmLiqYTwNQqplwONdXTObbUaIBoIgYgEtvOXhbwKrzIFKAxwCeWAjqrYyrVcIqtxrNjGfDHSwyCUUfdfbrAqcayBLFHRKzRhvefcScmjidYOeigtAcDXsmoIpaPnvieOwwPeptugIcfLGXslcXHDAIjzzCkDqjuMNffPNmrEAsHLXkzxlFJwOAxiKeqrkVLlmphuzYQCDXtLdUdrQIzWcSXcnrzBdmpVtQzVUYsZTwjMhbaHGPdMbahWWDDcssNiWSnYhewugVKzEwXIXKFCSMFzlNVPwXTmyyWYdHMcxKAkogRMFjfrOTFSbsoVKfDkKhaAaEIgmHRDIDvttdefZWJqhwRgLvMohGMYSEAkDNBmQgEWdOTxAGbhaqKOdtzjBpPyUhDqfYgomCAoGEtWvOwleRkZSeeaiqvZAZQMxafSQudKBzBOVcIxgCZWJtzyDtnbMiItFMUuJoGyCJmguFLpcPxQjdQtbIdicwcWGpVCbZUymvlwVQwARDJoPKOXZHQxqzMUwpphPsjgwxuMVRdKvFRNVPasQqBAbhkIwgLLJUzbjOUqFCDHTGqgVjCWZrhLXfaWqSNURFELSvMyKCFUwDwbvsaQNdzocCdvWvParZcPKAdHFIUCallYacjQqiFiSPWlrQlBLJEXZuxlPBxLxenAlLkrSigtWdcrEPASmxCPtIHQQogDJQcKhDHBBzqmzzkmnRbyaKpFHvJoFQldJRKleCLJZtMkDdNFoXpzIScnEcKjooVbdfzRvAZvmfjTbwCIikfvPWqQxuOvyFSXdSYOYMIXyINMoUZtrSzAEsIeOwPOtoGfEyWXYhYvZKEaNIHBUlzeexwnvQSyFiYhPelNDotBySzeXiKyDHfJJikIjSGrrnSiEurapMjfZqdzMiFFkAZyLVHPLEsqbNdAroAcdlhlGtivccEXbqnmMsuzkOItVpvgHmoEdlLBCAceIWzPcECDZeUVxROTRsxDuowcxsPhVHrKOLtxOSKeuTSpemXOwsiqUIrdAGyhTvexzacczSCTqHylWrkjvsYnsvRjwSVPvvHrysuWaveDbuqUnZdYdchWWAOAxMTYLUFYQjNdZdlBXrBxiLVODbEAmKxDovVbxonGsnLBejVZiysqMcBviYyqEltUBpzjcItHFEASujjgNPBkokfwfgqrcCSaLzIajNkiQLYykkMemcytqvQNcDnJdCQMcTLnwTBTCAZzRFHWnVRPJUwivmPJVkfLPqPduhKqzZasvAmQyhSYBCkdVBmqzdmaGZQjjaGQxoJrbcCdUtqsICEKAVsXFrKjVDoZVnDoUksgSIJVuvrVEqnaWhxfurbjCVoPnuDBothwYJrWmCIglbieInvpJrGKmVfPKhgwxvJUhICTlxxfEjYLrbUEPoktCbEXZTmjcHKNNYHSdIlNpjurEDsXwEgxuAYGfmMZJoMOaHHLdrpREnqWqxFApXVunDDKoCGmykbsTcvJTzVlIlyfWPWQRgUtbADBvQEErmcESRtIrkdlMxYBAUgAVVgCuIYiwKRzwprExTurpbiFvsiqjWlVGUTDBacHgerdjTuLQqsgozjTTexwvamksVRwgpFTFzfMNxctDXyoBvMtagDVrmXyTKILXdyBxnKlVoaVAxhnBHmEsLOwQgRtpUYFIVteOaUWChdkXyfjaFAZtzUyZFhiFcYfSgUPkTdyIYsDeofRbZGoGsgkiyXwzWcEeAgnWKFuoIADqOXKYRYGatZZVjRbJOGVFTwfsTogQwtSJlnfKPJvOwaTbmryDagFRBuopANdXYZrCfdyEKIwTGTYfupxFUkuroJXEoPTPJkVzUPIeqGqOrzdarUKesjgggcRmAQKhUbicDBjLVriEFzLVXQrVwJEMBdZYOXdAgVFBmupxvMeTkPCUmyqeJySNdqHhEjUtVQhZLNirQThSMoCwOknAbLSDEMWkCvWycOxFLylQwcZtQIrRaXjLtOKEsvXxtWwEVqFryMYIelENSGKBeGOcoqjdbIDpoumPXtmViFxzaQGbPFmtBiKWIJBvpFGVVAHPXHnBqQofuShApLBbixpaMFEJdsRdWynlLqvtZvByfujmOOYobmoNtbUohVRLAxvLjbdImNyQdxcDqnKKHZrlEXAhLTuwDPlmmdsCZGGNKBKCXvNPSjbvLrGYQGjxQXAqYaafeEHfcqKdFIKhhgpSVbwrlQiKusIalekjsVMkXACvzTFoomZdTKAvJqHbUcUcVYHqKgeytguyWsxKjfXmQWbQChvaqKibRXRTOlICioGSlhfFjKnVOagjciybDGXxjNAETalFrwqzkcnbBdNFkVhVVuUlMtMpwmKUokDeAeQWqbAollkOkyJqnpbpDknsZYYgVHJvBGCWhpqsxXsALIrkpzRSeixvXoqbBWDFurDQzXiJtjwlLfhKUPDpliGtjIoTklvCsQGEAZUHhrzWOaqzUwLeNcOSzAvuVhEmnGCTlFtPKfpxSgZwUEoISudyZdpEXVEeSIunjfocBVTPSKjyduTpDWQMKmrFrwdzLadeQPLbklUkbgkLxNkbICyqJuMRDIiFjlJngAljEppKzkfvMPyUMUAtGsGNAiChlOIIelmKiiZzvWoGqGMbWnSjiNKzBeSSniXlHpYNnPHfoFgZRIPSKEEWStkvqrBSWKIZwccFkEVQaxWJUKjUFqZpxsTDuYbDnzXfFLyxvxkdWFWOfAsspnzOGtSeqTSSdtMTENIVQZfqmsOaIcfNShDPEJJfrxPSJHRyBzAEgqGPDPkQAiOdQsiFkBJJlsMXSRikfCoPWyJUClRmRquVXEFKZNrbtZLzHBEkMfxHYztUlElkRbKSdceSHTtiikpvdAKQHzYkOastqjOeeFCOSEsbCpDPlfUjbKySzjtDTftPwBTOZfprDxsUzAMdmjCJMTxYPpzfboQnZNNDXgVKAUWRyXTqiLafetPvmJLsUTEzpHTQTnhCtQBeriJXOfAsWqdDJaivoJbLGHuBUgvDoHteQZykbXUwSZrUCPEtztJFcAKmVPEAoKOoYbcviWVlSZOCkdraRrxhSattuyopxulwDMjkJdtWRkDCYNzjUOPmySxnRRbBVsFgTbxubphFCyksyZDXTirmfawnBRyTIWGfwZrzqbssqiZLMcCLEUIqCnYgmDEFyEEvOnQHybGsrMaIAVNzavEJtioVmVZshyjonkEHmxCdcnXQXpoWIJClaGXITgcfYGdyilGxssWiBZvHJSBjMSYexnLEQalzHJtIrXFmRDQuhdSNAtKeMJnAOKgMncfjeyBlVQpTUajawCDBPvotAqRrDuARuZZSyXLCiiTwRBMFZtvdqJDypprUWRRYJwMuKqIxHqBWfhMBdXSZSkItcdyrYRjmwoDkmAPeWldQoLQZFzqArOsHNBnvSQJDcmtmhoRoQVkYoFfrUPAaCfqItHujkzwHwmzyMWAKgMnIittgeMryyJMVkumNYoUZzDcPKwZoZVWISDPKYdwpuoBrLeevPSKkgGBbBYnrPMeLMsRvHghQifBZzxrenNfgHfcEpofxrJLiFFRgbBnHiDhqXCfBKqwyTAiLrYpKTfMjevNcshqLAxKsLeZqakzulUIdRpRwjnUJEDkEGleLdaONPkmnNTMOwfpfJclSlVrDXBPZGIKCmMlAbvxUBPzOceNwVDapjovcpxSDdvRqgiaMFhDyOSeGVuTECMUOOptFgvDdqwkOpexwayoGkXSOiRXzxdsBnodXgaKYKgiUrRXkvbyjMdfBPuAytbqXPKHhxrFFqUVtOqAasOTItzEvVwpYuRUmivmIlBbkvMdLYJYRHxgXzyTWIplGeqNEXJpQTpOGddkLJNUsmAyGzHaoyBMOsLfENQkaRFAtJChibHpoVfRTRIWzWyTTQVNxzoaZMkDIGDvzZahYviDginIAEhmddcQttIUQVkEpXjgMiHfKhXXHxfujUnUHaxunMYSulZafceNKxaTawWkaFCnkugsGIOcuznPuGHyVQUzJEKTezwTbqGFtqUkrzUIXaNJATrsv" +
          "TTDWApTjpMukfxemJPdARoBUHWOrSDZWEXpvfLxsqDYBvjswqgeAOcDsHdWGIJbMSENxrEoGkBDpAwHYEROwmjkKAGbnPzwcwSUN" +
          "RODnOkKHxMdmpfWSuyllNthdVQJRCmzquRetdzQeImGQdaNwgAsrADxrvJJVEmtDHqRryjxwrHOPwSVYWnseHYAPRULCrScMwIKbNPJIYwp" +
          "usBXIlxSmbZtbWbLhwIvHKogSXDrHGsRBloxpteyLmVlYaFvHgapmNPuTDfsMsrGQJucqrBiPXopFEchQlrdeziYQGbiPyAABhMTDgOzU" +
          "LBuVccwNzoHFGZewKpKoigbiCtdonRyMNsYXzRjNxsFNgiTBQMYXpOdXZmKKllqkBuocKjESuUUTwfILitGSdWEyHKCrrdGnTcVqDpZKlZ" +
          "DtXdjFWGESaeGamRjladRsKzVXllOILWzzZOqRyaomXivEUVaeQYXdbODMrGFlhxKiDenNDmVGzwUKeAucoyiFbhwIpLxegPCnGRkMPzCr" +
          "hnkDMhvGpNavBogmJjPYwOSeqYSAmedHWNxTHpKNKAtHUBpbUGxWYSGHcektblczkPSSJSceSfUCLzgEAAswRCDLcVqUlqvGgeVuIWOpdD" +
          "uJjwqRNLvszhaGUEIyRJtDYkQHsGNORzFWItKMubzgrZEhfYrQMsjDxxnFzJRnVlHcGnaLexdPkyAdPSlpfWhxwPDxLQduheHlOFPKORHq" +
          "XWoMdaWQqThvgslRCMPXBbnLWBlzMdkFpUGIfeWTXgIIYZEKYtOjicsauTLKjcreKigUGKBFRDnpyKoWkljemDCipMzYBeIZAFKckNapDM" +
          "VNkBKqCiIAvYlYixnDsppuCVpUaPvPuvpCnGTvkGitZyHgpGQCGHnKpiOLmURypCCzfwnbtXPjbWNmhfUBzBaMbOUAvLQqOeWEpRbnpPnT" +
          "bvYDeRfGuYajdhTqphPYaAPFmcbbPWHnfnORxjGuOiTqGnKKsQwauvfvzkJnZKohToAHCfFTprgGnzDxNzaXNKmJtzeyAScsuxbaMhcxzg" +
          "cbqKOmfSUrqqpnpwHnQtOBRuCJXEjZgrrDlkwUUmkaXZDNX", 22, 160L, "220", 170F, "")
      )
      var seq_df = spark.createDataFrame(sseq).toDF("name", "age", "mock_col1", "income", "mock_col2", "mock_col3")
      var res = et.train(seq_df, "", Map())
      res.show()
      print(res.collectAsList().get(2).get(1).toString)

      val data_seq2 = Seq(
        (1, "aa", 13, null, "kkk"),
        (2, "bb", 12, null, null),
        (3, "cc", 15, null, "cc"),
        (4, "dd", 21, "s", null),
        (5, "ee", 18, "hyt", "gg"),
        (7, "ff", 10, "eqwewq", ""),
        (8, "dd", 19, null, "bdbds"),
        (9, "hh", 10, "4eweq", ""),
        (6, "ff", 18, null, "bvd"),
      )
      var df2 = spark.createDataFrame(data_seq2).toDF("passangerId", "name", "age", "cabin", "embarked")
      var res2 = et.train(df2, "", Map())
      res2.show()
      print(res2.collectAsList().get(2).get(1).toString)
    }
  }

}
